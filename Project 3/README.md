# Project Summary
A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.
As their data engineer, you are tasked with building an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow their analytics team to continue finding insights in what songs their users are listening to.
You'll be able to test your database and ETL pipeline by running queries given to you by the analytics team from Sparkify and compare your results with their expected results.
# Project Description
In this project, you'll apply what you've learned on Spark and data lakes to build an ETL pipeline for a data lake hosted on S3. To complete the project, you will need to load data from S3, process the data into analytics tables using Spark, and load them back into S3. You'll deploy this Spark process on a cluster using AWS.
# Architecture
This project uses spark to read file and extract table on local/cluster. 
Extract -> Define -> Filter -> Write parquet table enter folder path output_data in local/cluster
Path output in local: data/output_data/table_name + now/
now = datetime.now().strftime('%Y-%m-%d-%H-%M-%S-%f').

![](./img/path.png)
# Database schema
We use star schema
### Dimention Tables
    1. users_table
    2. songs_table
    3. artists_table
    4. times_table
### Fact Tables
The songplays table is the core of this schema and contains foreign keys to four tables:

``
start_time REFERENCES time(start_time)
user_id REFERENCES time(start_time)
song_id REFERENCES songs(song_id)
artist_id REFERENCES artists(artist_id)
``
# About Project
dl.cfg contain config value connect to aws, path input/output file in local/cluster
etl.py: Load file json -> read file -> Extract columns for tables -> write parquet file.

test.ipynb: Use for test code before 
# Run Project 
Run file test.ipynb
command: python3 etl.py
Output consists of:
- Start function create_spark_session, process_song_data, process_log_data
- Read file song_data, log_data
- Extract columns for tables
- Write parquet file
 
