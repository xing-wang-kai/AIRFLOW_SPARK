from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from os.path import join

import argparse

def create_table(df):
    return (df.alias('tweet')
              .groupBy(f.to_date('created_at').alias("create_date")).agg(
                    f.countDistinct('author_id').alias('n_usuarios'),
                    f.sum('like_count').alias('n_like'),
                    f.sum('quote_count').alias('n_quote'),
                    f.sum('reply_count').alias('n_reply'),
                    f.sum('retweet_count').alias('n_retweet')
                ).withColumn('weekday', f.date_format('create_date', 'E')))

def save_results(df, dest):
    df.coalesce(1).write.mode('overwrite').json(dest)


def save_extraction(spark, src, dest, process_data):
    df = spark.read.json(join(src, f'process_data={process_data}'))

    insight_df = create_table(df)
    
    destine_path = join(dest, "{table_name}", f"process_data={process_data}")

    save_results(insight_df, destine_path.format(table_name='insight_tweeter'))

if __name__ == '__main__':

    parser = (argparse.ArgumentParser(
        description='GOLD_TRANSFORMA_T'
    ))

    parser.add_argument("--src", required=True)
    parser.add_argument("--dest", required=True)
    parser.add_argument("--process-data", required=True)

    args = parser.parse_args()

    spark = (SparkSession.builder
            .appName('GOLD_TRANSFORM_T')
            .getOrCreate()
            )
    save_extraction(spark, 
                    args.src, 
                    args.dest, 
                    args.process_data
                    )