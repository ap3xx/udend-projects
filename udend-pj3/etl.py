import configparser
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, from_unixtime

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Builds Spark session
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .config("spark.sql.shuffle.partitions", 30)\
        .getOrCreate()
    return spark


@udf
def get_timestamp_sec(ts):
    """
    Returns timestamp in seconds
    """
    return ts//1000


def process_song_data(spark, input_data, output_data):
    """
    Loads song_data from S3 and processes it by extracting the songs and artist tables
    then saving to S3

    Parameters:
        spark       : this is the Spark Session
        input_data  : the location of song_data from where the file is load to process
        output_data : the location where after processing the results will be stored
    """
    song_data = input_data + 'song_data/*/*/*/*.json'
    df = spark.read.json(song_data)
    df.createOrReplaceTempView("song_data_table")

    song_query = """
        SELECT DISTINCT song.song_id, 
            song.title,
            song.artist_id,
            song.year,
            song.duration
        FROM song_data_table song
        WHERE song_id IS NOT NULL
    """
    songs_table = spark.sql(song_query)
    songs_table.write.mode('append').partitionBy("year", "artist_id").parquet(output_data+'songs_table/')

    artists_query = """
        SELECT DISTINCT song.artist_id, 
            song.artist_name,
            song.artist_location,
            song.artist_latitude,
            song.artist_longitude
        FROM song_data_table song
        WHERE song.artist_id IS NOT NULL
    """
    artists_table = spark.sql(artists_query)
    artists_table.write.mode('append').parquet(output_data+'artists_table/')


def process_log_data(spark, input_data, output_data):
    """
    Loads log_data from S3 and processes it by extracting user and time tables
    then saving to S3. It also creates the songplays table by merging everything from this and
    process_song_data function

    Parameters:
        spark       : this is the Spark Session
        input_data  : the location of song_data from where the file is load to process
        output_data : the location where after processing the results will be stored
    """
    log_path = input_data + 'log_data/*.json'
    df_raw = spark.read.json(log_path)
    
    df = df_raw.where(df_raw.page == 'NextSong').withColumn("timestamp", from_unixtime(get_timestamp_sec(col("ts"))))
    df.createOrReplaceTempView("log_data_table")

    users_query = """
        SELECT log.userId AS user_id, 
            log.firstName AS first_name,
            log.lastName AS last_name,
            log.gender AS gender,
            log.level AS level,
            log.timestamp
        FROM log_data_table log
        WHERE log.userId IS NOT NULL
    """
    users_table = spark.sql(users_query)
    users_dedup = users_table.orderBy('timestamp').dropDuplicates(subset=['id']).drop('timestamp')
    users_dedup.write.mode('append').parquet(output_data+'users_table/')

    time_query = """
        SELECT DISTINCT
            ts AS start_time, 
            timestamp,
            cast(date_format(timestamp, "HH") AS INTEGER) AS hour,
            cast(date_format(timestamp, "dd") AS INTEGER) AS day,
            weekofyear(timestamp) AS week,
            month(timestamp) AS month,
            year(timestamp) AS year,
            dayofweek(timestamp) AS weekday
        FROM log_data_table
    """
    time_table = spark.sql(time_query)
    time_table.write.mode('append').partitionBy("year", "month").parquet(output_data+'time_table/')

    songplays_query = """
        SELECT DISTINCT log.ts AS start_time,
            month(timestamp) AS month,
            year(timestamp) AS year,
            log.userId AS user_id,
            log.level AS level,
            song.song_id AS song_id,
            song.artist_id AS artist_id,
            log.sessionId AS session_id,
            log.location AS location,
            log.userAgent AS user_agent
        FROM log_data_table log
        JOIN song_data_table song
            ON log.artist = song.artist_name 
            AND log.song = song.title
            AND log.length = song.duration
    """
    songplays_table = spark.sql(songplays_query)
    songplays_table.write.mode('append').partitionBy("year", "month").parquet(output_data+'songplays_table/')


def main():
    spark = create_spark_session()
    bucket_name = 'udacity-dend'
    output_prefix = 'datalake'
    input_data = "s3a://{}/".format(bucket_name)
    output_data = "s3a://{}/{}/".format(bucket_name, output_prefix)
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
