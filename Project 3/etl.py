"""Run file ETL."""
import configparser
from datetime import datetime
import os
import zipfile
import pyspark.sql.functions as f
from pyspark.sql import types as t
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from zipfile import ZipFile


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """Create a Apache Spark session to process the data.
    
    Keyword arguments:
    * N/A
    Output:
    * spark -- An Apache Spark session.
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data_song_data, output_data):
    """Load JSON input data (song_data) from input_data path.
    
    process the data to extract song_table and artists_table, and
    store the queried data to parquet files.
    Keyword arguments:
    * spark         -- reference to Spark session.
    * input_data    -- path(local/s3) to input_data to be processed (song_data)
    * output_data   -- path(local/s3) to location to store
                       the output (parquet files).
    Output:
    * songs_table   -- directory with parquet files
                       stored in output_data path.
    * artists_table -- directory with parquet files
                       stored in output_data path.
    """
    print("START function process_song_data")
    # get filepath to song data file
    song_data = input_data_song_data
    # read song data file
    print("Start reading file song_data")
    df = spark.read.json(song_data).dropDuplicates()
    print("Finish reading file song_data")
    # extract columns to create songs table
    # Create and write songs_table
    print("Create and write songs_table")
    print("Create songs_table")
    df.createOrReplaceTempView("songs_table_df")
    songs_table = spark.sql("""
    SELECT
        song_id,
        title,
        artist_id,
        year,
        duration
    FROM songs_table_df
    ORDER BY song_id
    """)
    # write songs table to parquet files partitioned by year and artist
    # Write DF to Spark parquet file (partitioned by year and artist_id)
    print("Write songs table to parquet files partitioned by year and artist")
    now = datetime.now().strftime('%Y-%m-%d-%H-%M-%S-%f')
    songs_table_path = output_data + "songs_table" + "_" + now
    songs_table.write.partitionBy("year", "artist_id").\
        parquet(songs_table_path)
    print("Finish songs table to parquet files partitioned by year and artist")
    print("Create artists_table")
    # extract columns to create artists table
    df.createOrReplaceTempView("artists_table_df")
    artists_table = spark.sql("""
    SELECT
        artist_id as artist_id,
        artist_name as name,
        artist_location as location,
        artist_latitude as latitude,
        artist_longitude as longitude
    FROM artists_table_df
    ORDER BY artist_id DESC
    """)
    print("Write artists table to parquet files")
    # write artists table to parquet files
    artists_table_path = output_data + "artists_table.parquet" + "_" + now
    artists_table.write.parquet(artists_table_path)
    print("Finish write artists table to parquet files")
    print("END function process_song_data")


def process_log_data(spark, input_log_data, input_data_song_data, output_data):
    """Load JSON input data (log_data) from input_data path.
    
    process the data to extract users_table, time_table,
    songplays_table, and store the queried data to parquet files.
    Keyword arguments:
    * spark            -- reference to Spark session.
    * input_data       -- path to input_data to be processed (log_data)
    * output_data      -- path to location to store the output
                          (parquet files).
    Output:
    * users_table      -- directory with users_table parquet files
                          stored in output_data path.
    * time_table       -- directory with time_table parquet files
                          stored in output_data path.
    * songplayes_table -- directory with songplays_table parquet files
                          stored in output_data path.
    """
    print("START function process_log_data")
    # get filepath to log data file
    log_data = input_log_data

    # read log data file
    print("Start reading file log_data")
    df = spark.read.json(log_data).dropDuplicates()
    print("Finish reading file song_data")
    # filter by actions for song plays
    # Filter record page = 'NextSong'
    df_log_data_filtered = df.filter(df.page == 'NextSong')
    # extract columns for users table
    print("Create and write users_table, time_table, songplays_table")
    print("Create users_table")
    df_log_data_filtered.createOrReplaceTempView("users_table_df")
    users_table = spark.sql("""
    SELECT DISTINCT
        userId as user_id,
        firstName as first_name,
        lastName as last_name,
        gender,
        level
    FROM users_table_df
    ORDER BY last_name
    """)
    # write users table to parquet files
    print("Write users table to parquet files")
    now = datetime.now().strftime('%Y-%m-%d-%H-%M-%S-%f')
    users_table_path = output_data + "user_table.parquet" + "_" + now
    users_table.write.parquet(users_table_path)
    print("Finish write users table to parquet files")

    # create timestamp column from original timestamp column
    # Create a new column with timestamp
    @udf(t.TimestampType())
    def get_timestamp(ts):
        return datetime.fromtimestamp(ts / 1000.0)
    df_log_data_filtered = df_log_data_filtered.\
        withColumn("timestamp", get_timestamp("ts"))

    # Create a new column with datetime
    @udf(t.StringType())
    def get_datetime(ts):
        return datetime.fromtimestamp(ts / 1000.0).\
            strftime('%Y-%m-%d %H:%M:%S')
    df_log_data_filtered = df_log_data_filtered.\
        withColumn("datetime", get_datetime("ts"))
    # extract columns to create time table
    print("Create time_table")
    df_log_data_filtered.createOrReplaceTempView("time_table_df")
    time_table = spark.sql("""
    SELECT  DISTINCT
        datetime AS start_time,
        hour(timestamp) AS hour,
        day(timestamp)  AS day,
        weekofyear(timestamp) AS week,
        month(timestamp) AS month,
        year(timestamp) AS year,
        dayofweek(timestamp) AS weekday
    FROM time_table_df
    ORDER BY start_time
    """)
    # write time table to parquet files partitioned by year and month
    print("Write time table to parquet files")
    time_table_path = output_data + "times_table.parquet" + "_" + now
    time_table.write.mode("overwrite").partitionBy("year", "month")\
        .parquet(time_table_path)
    print("Finish time table to parquet files")
    # read in song data to use for songplays table
    print("Read song_data file")
    song_data = input_data_song_data
    df_song_data = spark.read.json(song_data)
    # Join song_data and log_data
    df_ld_sd_joined = df_log_data_filtered\
        .join(
            df_song_data,
            (df_log_data_filtered.artist == df_song_data.artist_name) &
            (df_log_data_filtered.song == df_song_data.title)
        )
    # extract columns from joined song and
    # log datasets to create songplays table
    print("Write songplays_table")
    df_ld_sd_joined = df_ld_sd_joined.\
        withColumn("songplay_id", f.monotonically_increasing_id())
    df_ld_sd_joined.createOrReplaceTempView("songplays_table_df")
    songplays_table = spark.sql("""
    SELECT
        songplay_id as songplay_id,
        timestamp as start_time,
        userId as user_id,
        level,
        song_id,
        artist_id,
        sessionId as session_id,
        location,
        userAgent as user_agent
    FROM songplays_table_df
    ORDER BY (user_id, session_id)
    """)
    # write songplays table to parquet files partitioned by year and month
    songplays_table_path = output_data + "songplays_table.parquet" + "_" + now
    time_table.write.mode("overwrite").partitionBy("year", "month").\
        parquet(songplays_table_path)
    print("Finish write songplays_table")
    print("END FUNCTION process_log_data")


def main():
    """Load JSON input data (song_data and log_data) from input_data path.
    
    process the data to extract songs_table, artists_table,
    users_table, time_table, songplays_table,
    and store the queried data to parquet files to output_data path.
    Keyword arguments:
    * NA
    Output:
    * songs_table      -- directory with songs_table parquet files
                          stored in output_data path.
    * artists_table    -- directory with artists_table parquet files
                          stored in output_data path.
    * users_table      -- directory with users_table parquet files
                          stored in output_data path.
    * time_table       -- directory with time_table parquet files
                          stored in output_data path.
    * songplayes_table -- directory with songplays_table parquet files
                          stored in output_data path.
    """
    spark = create_spark_session()
#     input_data = "s3a://udacity-dend/"
#     output_data = ""
    # Use path input from local
    input_data_song_data = config['LOCAL']['INPUT_DATA_SONG_DATA_LOCAL']
    input_log_data = config['LOCAL']['INPUT_DATA_LOG_DATA_LOCAL']
    output_data = config['LOCAL']['OUTPUT_DATA_LOCAL']
    process_song_data(spark, input_data_song_data, output_data)
    process_log_data(spark, input_log_data, input_data_song_data, output_data)


if __name__ == "__main__":
    main()
