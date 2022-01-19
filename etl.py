import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import *
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """

    :return:
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """

    :param spark:
    :param input_data:
    :param output_data:
    :return:
    """
    # get filepath to song data file
    song_data = 's3://udacity-dend/song_data/*/*/*/*.json'
    song_data_path = os.path.join(input_data, song_data)
    songs_schema = build_songs_schema()

    # read song data file
    df = spark.read.json(song_data_path, schema=songs_schema, multiLine=True, mode='PERMISSIVE', columnNameOfCorruptRecord='orrupt_record')

    # extract columns to create songs table
    songs_table = df.select("song_id", "title", "artist_id", "year", "duration").dropDuplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table

    # extract columns to create artists table
    artists_table = df.select("artist_id", "name", "location", "lattitude", "longitude").dropDuplicates()
    
    # write artists table to parquet files
    artists_table


def process_log_data(spark, input_data, output_data):
    """

    :param spark:
    :param input_data:
    :param output_data:
    :return:
    """
    # get filepath to log data file
    log_data = 's3://udacity-dend/log_data/*/*/*.json'
    log_data_path = os.path.join(input_data, log_data)
    log_schema = build_log_schema()

    # read log data file
    df = spark.read.json(log_data_path, schema=log_schema)
    
    # filter by actions for song plays
    df =  df.filter(df.page == "NextSong")

    # extract columns for users table    
    artists_table = 
    
    # write users table to parquet files
    artists_table

    # create timestamp column from original timestamp column
    get_timestamp = udf()
    df = 
    
    # create datetime column from original timestamp column
    get_datetime = udf()
    df = 
    
    # extract columns to create time table
    time_table = 
    
    # write time table to parquet files partitioned by year and month
    time_table

    # read in song data to use for songplays table
    song_df = 

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = 

    # write songplays table to parquet files partitioned by year and month
    songplays_table


def build_songs_schema():
    """

    :return:
    """
    schema = StructType([
        StructField("artist_id", StringType(), True),
        StructField("artist_latitude", DecimalType(), True),
        StructField("artist_location", StringType(), True),
        StructField("artist_longitude", DecimalType(), True),
        StructField("artist_name", StringType(), True),
        StructField("duration", DecimalType(), True),
        StructField("num_songs", IntegerType(), True),
        StructField("song_id", StringType(), True),
        StructField("title", StringType(), True),
        StructField("year", IntegerType(), True)
    ])

    return schema

def build_log_schema():
    """

    :return:
    """
    schema = StructType([
        StructField("artist_name", StringType(), True),
        StructField("user_auth_statuse", StringType(), True),
        StructField("user_first_Name", StringType(), True),
        StructField("user_gender", StringType(), True),
        StructField("item_In_Session", IntegerType(), True),
        StructField("user_last_Name", StringType(), True),
        StructField("length", DecimalType(), True),
        StructField("user_level", StringType(), True),
        StructField("location", StringType(), True),
        StructField("user_using_method", StringType(), True),
        StructField("user_staying_page", StringType(), True),
        StructField("registration", StringType(), True),
        StructField("session_Id", IntegerType(), True),
        StructField("song_name", StringType(), True),
        StructField("https_status", IntegerType(), True),
        StructField("ts", StringType(), True),
        StructField("user_Agent", StringType(), True),
        StructField("user_id", IntegerType(), True)
    ])

    return schema

def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = ""
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
