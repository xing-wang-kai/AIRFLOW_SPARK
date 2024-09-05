import sys
sys.path.append("airflow_twitter")

from airflow.models import BaseOperator, DAG, TaskInstance
import json
from hook.twitter_hook import TwitterHook
from datetime import datetime, timedelta
from os.path import join
from pathlib import Path

class TwitterOperator(BaseOperator):

    template_fields = ['query', 'file_path', 'start_time', 'end_time']
    
    def __init__(self, file_path, start_time, end_time, query, **kwargs):
        self.start_time = start_time
        self.end_time = end_time
        self.query = query
        self.file_path = file_path
        super().__init__(**kwargs)
    
    def create_parent_folder(self):
        (Path(self.file_path).parent).mkdir(parents=True, exist_ok=True)

    def execute(self, context):

        end_time= self.end_time
        start_time = self.start_time
        query = self.query

        self.create_parent_folder()

        with open(f"{self.file_path}.json", 'w') as twitter_files:
            for pg in TwitterHook(start_time, end_time, query).run():
                json.dump(pg, twitter_files,ensure_ascii=False)
                twitter_files.write("\n")

if __name__ == "__main__":

    FORMAT_DATE = "%Y-%m-%dT%H:%M:%S.00Z"
    end_time= datetime.now()
    start_time = (datetime.now() + timedelta(-1)).date()
    query = "data engineer"

    with DAG (
        dag_id='Twitter_extractor',
        start_date=datetime.now()
    )  as dag:
        twitter_operator = TwitterOperator(
                                            file_path=join("datalake/twitter_dataengineer", 
                                                           f"extract_date={datetime.now().strftime('%Y-%m-%d')}",
                                                           f"data_engineer_{datetime.now().strftime('%Y-%m-%d')}"),
                                            query=query, 
                                            start_time=start_time, 
                                            end_time=end_time, 
                                            task_id='Operator_function'
                                            )
        twitter_instance = TaskInstance(task=twitter_operator)

        twitter_operator.execute(twitter_instance.task_id)