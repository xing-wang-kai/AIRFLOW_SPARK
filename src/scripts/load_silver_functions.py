from pyspark.sql import functions as f
from pyspark.sql import SparkSession

from os.path import join
import argparse

def get_twitters(df):
    """this function generate new dataframe with tweets dataframe explode

    Args:
        df (spark Dataframe): receive dataframe will be explode

    Returns:
        spark dataframe: return dataframe with explode datas
    """

    tweet_df = (df.select(f.explode("data").alias("tweets"))
        .select("tweets.author_id", "tweets.conversation_id",
            "tweets.created_at", "tweets.id",
            "tweets.public_metrics.*", "tweets.text"))
    return tweet_df


def get_users(df):

    """this function generate new dataframe with tweets dataframe explode

    Args:
        df (spark Dataframe): receive dataframe will be explode

    Returns:
        spark dataframe: return dataframe with explode datas
    """
     
    users_df = (df.select(f.explode("includes.users").alias("users"))
        .select("users.created_at", "users.id",
                "users.name", "users.username")
                )
    return users_df


def save_results(df, dist):
    df.coalesce(1).write.mode('overwrite').json(dist)


def define_extration(spark, src, dist, process_data):

    df = spark.read.json(src)

    twitter_df = get_twitters(df)
    user_df = get_users(df)

    destine_path = join(dist, "{table_name}", f"process_data={process_data}")

    save_results(twitter_df, destine_path.format(table_name='twitter_df'))
    save_results(user_df, destine_path.format(table_name='user_df'))


if __name__ == '__main__':

    parser = (argparse.ArgumentParser(
        description="Spark Twitter Transformation"
    ))

    parser.add_argument("--src", required=True)
    parser.add_argument("--dest", required=True)
    parser.add_argument("--process-data", required=True)

    args = parser.parse_args()

    spark = (SparkSession.builder
             .appName("twitter_transformation")
             .getOrCreate()
             )
    
    define_extration(spark, 
                     args.src, 
                     args.dest, 
                     args.process_data)