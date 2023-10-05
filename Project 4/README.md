# Project Summary
A music streaming company, Sparkify, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Airflow.

They have decided to bring you into the project and expect you to create high grade data pipelines that are dynamic and built from reusable tasks, can be monitored, and allow easy backfills. They have also noted that the data quality plays a big part when analyses are executed on top the data warehouse and want to run tests against their datasets after the ETL steps have been executed to catch any discrepancies in the datasets.

The source data resides in S3 and needs to be processed in Sparkify's data warehouse in Amazon Redshift. The source datasets consist of JSON logs that tell about user activity in the application and JSON metadata about the songs the users listen to.
# Project Description
This project will introduce you to the core concepts of Apache Airflow. To complete the project, you will need to create your own custom operators to perform tasks such as staging the data, filling the data warehouse, and running checks on the data as the final step.

We have provided you with a project template that takes care of all the imports and provides four empty operators that need to be implemented into functional pieces of a data pipeline. The template also contains a set of tasks that need to be linked to achieve a coherent and sensible data flow within the pipeline.

You'll be provided with a helpers class that contains all the SQL transformations. Thus, you won't need to write the ETL yourselves, but you'll need to execute it with your custom operators.
# Prerequisites
Tables must be created in Redshift before executing the DAG workflow. The create tables statements can be found in:
create_tables.sql


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
create_tables.sql Contains the DDL for all tables used in this projecs
create_tables.py Contains script used connect DB, create table to redshift
sql_queries.py Define query used for create_tables.py
dwh.cfg Contains config connect to redshift, database
udac_example_dag.py - The DAG configuration file to run in Airflow
stage_redshift.py - Operator to read files from S3 and load into Redshift staging tables
load_fact.py - Operator to load the fact table in Redshift
load_dimension.py - Operator to read from staging tables and load the dimension tables in Redshift
data_quality.py - Operator for data quality checking

# Run Project 
Run file create_tables.py
command: python3 /airflow/create_tables.py
Run airflow
command: /opt/airflow/start.sh
 
