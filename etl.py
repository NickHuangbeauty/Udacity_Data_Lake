import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, from_unixtime, monotonically_increasing_id
from pyspark.sql.types import *
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS_SECRET_ACCESS_KEY']


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
    df = spark.read.json(song_data_path, schema=songs_schema, multiLine=True, mode='PERMISSIVE',
                         columnNameOfCorruptRecord='orrupt_record')

    # extract columns to create songs table
    songs_table = df.select("song_id", "title", "artist_id", "year", "duration").dropDuplicates()

    # write songs table to parquet files partitioned by year and artist
    # Method 1: TODO Check it
    songs_table = songs_table.write.partitionBy("year", "artist_id").parquet(os.path.join(output_data, ''), 'overwrite')
    # Method 2: TODO Check it
    songs_data = songs_table.write.partitionBy("year", "artist_id").format("parquet").mode("overwrite")\
                 .save(output_data)


    # extract columns to create artists table
    artists_table = df.select("artist_id", "name", "location", "lattitude", "longitude").dropDuplicates()

    # write artists table to parquet files : TODO Check it
    artists_table = artists_table.write.format("parquet").mode("overwrite") \
                    .save(output_data)


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
    df = df.filter(df.page == "NextSong")

    # extract columns for users table
    user_table = df.selectExpr("user_id", "user_first_name as first_name", "user_last_name as last_name",
                               "user_gender as gender", "user_level as level").dropDuplicates()

    # write users table to parquet files
    user_table = user_table.write.format("parquet").mode("overwrite") \
                 .save(output_data)

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: str(datetime.fromtimestamp(int(x) / 1000.0)))
    # get_timestamp = udf(lambda x: x/1000.0, IntegerType()) # TODO: Check this using mwthod!
    df = df.withColumn("start_time", get_timestamp(df.selectExpr("ts")))
    # df = df.withColumn("start_time", get_timestamp(col("ts"))) # TODO: Check this using mwthod!
    # df = df.withColumn("start_time", get_timestamp(df.ts)) # TODO: Check this using mwthod!
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: from_unixtime(x), TimestampType())
    df = df.withColumn("datetime", get_datetime(col('ts')))

    # extract columns to create time table
    time_table = df.select("start_time").dropDuplicates() \
        .withColumn("hour", hour(col("start_time").cast(TimestampType()))) \
        .withColumn("day", dayofmonth(col("start_time").cast(TimestampType()))) \
        .withColumn("week", weekofyear(col("start_time").cast(TimestampType()))) \
        .withColumn("month", month(col("start_time").cast(TimestampType()))) \
        .withColumn("year", year(col("start_time").cast(TimestampType()))) \
        .withColumn("weekday",
                    date_format(col("start_time").cast(TimestampType()), 'E'))  # TODO: Why it add 'E' parameter?

    # write time table to parquet files partitioned by year and month
    time_table = time_table.write.partitionBy("year", "month").format("parquet").mode("overwrite") \
                 .save(output_data)

    # read in song data to use for songplays table
    song_df = spark.sql("SELECT song_id, title, artist_id, year, duration FROM songs_table")

    # extract columns from joined song and log datasets to create songplays table
    # TODO: Check what is the main table?
    songplays_table = df.join(song_df, (df.song_name == song_df.title) & \
                                       (df.artist_name == song_df.artist_name) & \
                                       (df.length == song_df.duration), 'left_outer').select(
                                    monotonically_increasing_id().alias("songplay_id"),
                                    col("start_time"),
                                    col("user_id"),
                                    col("user_level").alias("level"),
                                    col("song_id"),
                                    col("artist_id"),
                                    col("session_id"),
                                    col("location"),
                                    col("user_agent")).withColumn("month", month(col("start_time"))) \
                                                      .withColumn("year", year(col("start_time")))

    # write songplays table to parquet files partitioned by year and month
    songplays_table = songplays_table.write.partitionBy("year", "month").format("parquet").mode("overwrite") \
                      .sace(output_data)


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
