import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, from_unixtime, monotonically_increasing_id
from pyspark.sql.types import *
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format

# Access AWS Cloud configure
# config = configparser.ConfigParser()
# config.read('dl.cfg')

# os.environ["AWS_ACCESS_KEY_ID"] = config["ACCESS"]["AWS_ACCESS_KEY_ID"]
# os.environ["AWS_SECRET_ACCESS_KEY"] = config["ACCESS"]["AWS_SECRET_ACCESS_KEY"]


def create_spark_session():
    """
    Purpose:
        Build an access spark session for dealing data ETL of Data Lake
    :return: spark session
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    # .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    # .config("spark.hadoop.fs.s3a.awsAccessKeyId", os.environ['AWS_ACCESS_KEY_ID']) \
    # .config("spark.hadoop.fs.s3a.awsSecretAccessKey", os.environ['AWS_SECRET_ACCESS_KEY'])

    return spark


def process_song_data(spark, input_data, output_data):
    """
    Purpose:
        create songs_table and artists_table
        source table : song dataset
    :param spark: spark session
    :param input_data: Read source datasets
    :param output_data: Save result data
    :return: None
    """
    # get filepath to song data file
    song_data = 'song_data/*/*/*/*.json'
    song_data_path = os.path.join(input_data, song_data)
    songs_schema = build_songs_schema()

    # read song data file
    df = spark.read.json(song_data_path, schema=songs_schema, multiLine=True, mode='PERMISSIVE',
                         columnNameOfCorruptRecord='corrupt_record')

    # extract columns to create songs table
    songs_table = df.select("song_id", "title", "artist_id", "year", "duration").dropDuplicates()

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id"). \
        parquet(os.path.join(output_data, "songs_table/"), "overwrite")

    # extract columns to create artists table
    artists_table = df.select("artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"). \
        dropDuplicates()

    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data, "artists_table/"), "overwrite")

    # create songs temp table for using join the songplays table


#     df.createOrReplaceTempView("song_temp_table")

def process_log_data(spark, input_data, output_data):
    """
    Purpose:
        create user_table, time_table and songplays table
        source table : log dataset
    :param spark: spark session
    :param input_data: Read source datasets
    :param output_data: Save result data
    :return: None
    """
    # get filepath to log data file
    log_data = 'log_data/*/*/*.json'
    log_data_path = os.path.join(input_data, log_data)
    log_schema = build_log_schema()

    # read log data file
    df = spark.read.json(log_data_path, schema=log_schema)

    # filter by actions for song plays
    df = df.filter(df.page == "NextSong")

    # extract columns for users table
    user_table = df.selectExpr("userId as user_id", "firstname as user_first_name", "lastname as user_last_name",
                               "gender as user_gender", "level as user_level").dropDuplicates()

    # write users table to parquet files
    user_table.write.format("parquet").mode("overwrite").save(os.path.join(output_data, "user_table/"))

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x / 1000.0).strftime('%Y-%m-%d %H:%M:%S'))
    df = df.withColumn("timestamp", get_timestamp(df.ts))

    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(x / 1000).strftime('%Y-%m-%d'))
    df = df.withColumn("datetime", get_datetime(df.ts))

    # extract columns to create time table
    time_table = df.select('timestamp', hour('datetime').alias('hour'),
                           dayofmonth('datetime').alias('day'),
                           weekofyear('datetime').alias('week'),
                           month('datetime').alias('month'),
                           year('datetime').alias('year'),
                           date_format('datetime', 'F').alias('weekday')
                           )

    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month").format("parquet").mode("overwrite"). \
        save(os.path.join(output_data, "time_table/"))

    song_df = spark.read \
        .format("json") \
        .option("basePath", os.path.join(input_data, "song_data/")) \
        .load(os.path.join(input_data, "song_data/*/*/*/*.json")).select("song_id",
                                                                         "title",
                                                                         "artist_id",
                                                                         "year",
                                                                         "duration",
                                                                         "artist_name").dropDuplicates()

    # extract columns from joined song and log datasets to create songplays table
    songplays_table = df.join(song_df, (df.song == song_df.title) & \
                              (df.artist == song_df.artist_name) & \
                              (df.length == song_df.duration), 'left_outer') \
        .select(monotonically_increasing_id().alias("songplay_id"),
                col("timestamp").alias("start_time"),
                col("userId").alias("user_id"),
                col("level").alias("user_level"),
                song_df.song_id,
                song_df.artist_id,
                col("sessionId").alias("session_id"),
                df.location,
                col("userAgent").alias("user_agent"),
                year("datetime").alias("year"),
                month("datetime").alias("month")
                )

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year", "month").format("parquet").mode("overwrite"). \
        save(os.path.join(output_data, "songplays_table/"))


def build_songs_schema():
    """
    Purpose:
        Build songs schema from the song data source and defined fields
    :return: Schema -> StructType Object
    """
    schema = StructType([
        StructField("num_songs", IntegerType(), True),
        StructField("artist_id", StringType(), True),
        StructField("artist_latitude", DecimalType(), True),
        StructField("artist_longitude", DecimalType(), True),
        StructField("artist_location", StringType(), True),
        StructField("artist_name", StringType(), True),
        StructField("song_id", StringType(), True),
        StructField("title", StringType(), True),
        StructField("duration", DecimalType(), True),
        StructField("year", IntegerType(), True)
    ])

    return schema


def build_log_schema():
    """
    Purpose:
        Build logs schema from the log data source and defined fields
    :return: Schema -> StructType Object
    """
    schema = StructType(
        [
            StructField("artist", StringType(), True),
            StructField("auth", StringType(), True),
            StructField("firstName", StringType(), True),
            StructField("gender", StringType(), True),
            StructField("itemInSession", StringType(), True),
            StructField("lastName", StringType(), True),
            StructField("length", DecimalType(), True),
            StructField("level", StringType(), True),
            StructField("location", StringType(), True),
            StructField("method", StringType(), True),
            StructField("page", StringType(), True),
            StructField("registration", LongType(), True),
            StructField("sessionId", IntegerType(), True),
            StructField("song", StringType(), True),
            StructField("status", IntegerType(), True),
            StructField("ts", LongType(), True),
            StructField("userAgent", StringType(), True),
            StructField("userId", StringType(), True)
        ])

    return schema


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3://mywsbucketbigdata/project_tables"

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)
