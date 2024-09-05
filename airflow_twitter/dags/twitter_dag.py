import sys
sys.path.append('airflow_twitter')

from airflow.models import DAG
from os.path import join
from operators.twitter_operator import TwitterOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from airflow.utils.dates import days_ago

with DAG (
        
        dag_id='Twitter_extractor_DAGs',
        start_date= days_ago(5),
        schedule_interval= '@daily'

    )  as dag:
        
        BASE_FOLDER = "datalake/{layer}/data_engineer/{partition}"

        PARTITION_FOLDER = "extract_data={{ data_interval_start.strftime('%Y-%m-%dT%H:%M:%S.00Z') }}"
        
        query = "data engineer"

        bronze_process = TwitterOperator(
                                    task_id='BRONZE_LAYER_EXTRACTION_E',
                                    file_path=join( BASE_FOLDER.format(layer='bronze', partition=PARTITION_FOLDER),
                                                    "data_engineer_{{ ds_nodash }}"),
                                    query=query,  
                                    start_time="{{ data_interval_start.strftime('%Y-%m-%dT%H:%M:%S.00Z') }}",
                                    end_time="{{ data_interval_end.strftime('%Y-%m-%dT%H:%M:%S.00Z') }}"

                                        
                                        )
        
        silver_process = SparkSubmitOperator(
                                    task_id='SILVER_LAYER_LOAD_L',
                                    application='src/scripts/load_silver_functions.py',
                                    name='twitter_transformation',
                                    application_args=[
                                            '--src', BASE_FOLDER.format(layer='bronze', partition=PARTITION_FOLDER),
                                            '--dest', BASE_FOLDER.format(layer='silver', partition=''), 
                                            '--process-data', '{{ ds_nodash }}'
                                            ]
                                    )
        
        gold_process = SparkSubmitOperator(
                                    task_id='GOLD_LAYER_TRANSFORM_T',
                                    application='src/scripts/transform_gold_functions.py',
                                    name='GOLD_TRANSFORM_T',
                                    application_args=[
                                            '--src', f"datalake/silver/data_engineer/twitter_df/",
                                            '--dest', BASE_FOLDER.format(layer='gold', partition=''), 
                                            '--process-data', '{{ ds_nodash }}'
                                            ]
                                    )
        
        bronze_process >> silver_process >> gold_process
  #       twitter_instance = TaskInstance(task=twitter_operator)
  #
  #      twitter_operator.execute(twitter_instance.task_id)