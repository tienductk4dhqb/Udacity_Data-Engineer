# Project Summary
A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.
# Project Description
In this project, you'll apply what you've learned on data warehouses and AWS to build an ETL pipeline for a database hosted on Redshift. To complete the project, you will need to load data from S3 to staging tables on Redshift and execute SQL statements that create the analytics tables from these staging tables.
# Architecture
This project uses 2 AWS services - S3 and Redshift.
Data sources come from two public S3 buckets (user activity and songs metadata) in the form of JSON files.
# Database schema
We use star schema
##  Used for two table coppy data in file from S3 to redsift database sparkify
    1. events_staging -> storage data  object log_data
    2. songs_staging -> storage data object song_data
### Dimention Tables
    1. users
    2. songs
    3. artists
    4. times
### Fact Tables
The songplays table is the core of this schema and contains foreign keys to four tables:

``
start_time REFERENCES time(start_time)
user_id REFERENCES time(start_time)
song_id REFERENCES songs(song_id)
artist_id REFERENCES artists(artist_id)
``
# About Project
dwh.cfg contain config value connect to redshift, uri, role...

![](./img/dwh.png)

create_tables.py: Connect database sparkify on redshift, bussiness drop, create table.
etl.py: Connect database sparkify on redshift, contain function coppy data, insert data
sql_queries.py: Define native SQL, used for create_tables.py and etl.py
test.ipynb: Run all project, test open/close connect, query sql, create redshift, role, s3, delete redshift, role...
# Run Project 
Run ALL file test.ipynb
Output consists of:
- Profile redshift
- Create Role
- Check Data In S3
- Create Redshift
- Check Status Redshift
- Get End Point Redshift
- Check Data After Run Script
- Delete RedShift
- Delete Role
 
