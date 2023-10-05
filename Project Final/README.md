# Project summary
* Analyze data on immigration, temperature, airport, demographics.
* Check and filter data, using create 1- 1, 1 - n relationships between data.
* Create etl store on AWS, test after running elt.
* The above data analysis should be updated when.

The project follows the follow steps:
* Step 1: Scope the Project and Gather Data
* Step 2: Explore and Assess the Data
* Step 3: Define the Data Model
* Step 4: Run ETL to Model the Data
* Step 5: Complete Project Write Up.
# Project description.
1. etl.py - reads data from S3, processes that data using Spark, and writes processed data as a set of dimensional tables back to S3
2. Capstone Project Template.ipynb read, show, count, check data before define data in etl store on AWS
3. DataQuality.ipynb Check data after run ETL
4. aws.cfg contains config in S3
### Step 1: Scope the Project and Gather Data
**Use Data Sets**
* **I94 Immigration Data**: This data comes from the US National Tourism and Trade Office.
* **World Temperature Data**: This dataset came from Kaggle.
* **U.S. City Demographic Data**: This data comes from OpenSoft.
* **Airport Code Table**: This is a simple table of airport codes and corresponding cities
### Step 2: Explore and Assess the Data
 **Tools**
1. AWS S3: data storage
2. Python for data processing
3. Pandas - exploratory data analysis on small data set
4. PySpark - data processing on large data set
**Explore the Data**.

* Use pandas for exploratory data analysis to get an overview on these data sets.
* Check null and duplicate using pandas.
* Split data sets to dimensional tables and change column names for better understanding.
* Utilize PySpark on one of the SAS data sets to test ETL data pipeline logic.

### Step 3: Define the Data Model
Map out the conceptual data model and explain why you chose that model
List the steps necessary to pipeline the data into the chosen data model
Map out the conceptual data model and explain why you chose that model

**Star Schema**
![](./img/diagram.png)
**Why you chose that model**

* Split into sub-tables for easy querying later 1 - 1.
Example:
fact_immigration 1 - 1 dimension_personal
fact_immigration 1 - 1 dimension_airline
* When the user only wants to get the list of people in the immagration, instead of having to query the whole table, the user only needs to query at dimension_personal.
* See 1-n relationships between tables.

Example:
dimention_mode 1 - n fact_immigration
Get name mode columns in fact_immigration
### Step 4  Run Pipelines to Model the Data
**4.1 Create the data model**
* Build the data pipelines to create the data model.
* Run data with spark and store in S3
* Define fact and dimension table

![](./img/evd.png)
**Refer etl.py**

**4.2 Data Quality Checks**

* Explain the data quality checks you'll perform to ensure the pipeline ran as expected. These could include:
* Unit tests for the scripts to ensure they are doing the right thing
* Source/Count checks to ensure completeness
![](./img/evd2.png)

**Refer DataQuality.ipynb**

**4.3 Data dictionary**

Create a data dictionary for your data model. For each field, provide a brief description of what the data is and where it came from. You can include the data dictionary in the notebook or in a separate file.
![](./img/evd3.png)

**Refer description_columns.txt**
### Step 5: Complete Project Write Up
* Clearly state the rationale for the choice of tools and technologies for the project.
* Propose how often the data should be updated and why.
* Write a description of how you would approach the problem differently under the following scenarios:
 * The data was increased by 100x.
 * The data populates a dashboard that must be updated on a daily basis by 7am every day.
 * The database needed to be accessed by 100+ people.
 
### Tools and Technologies
1. AWS S3 for data storage
2. Pandas for sample data set exploratory data analysis
3. PySpark for large data set data processing to transform staging table to dimensional table

### Propose how often the data should be updated and why.
1. All tables should be update in an append-only mode.
2. Airport should be updated daily, because of the high frequency
3. The temperature should be updated every year, as it doesn't change much

### Write a description of how you would approach the problem differently under the following scenarios:
**The data was increased by 100x.**
If Spark with standalone server mode can not process 100x data set, we could consider to put data in AWS EMR which is a distributed data cluster for processing large data sets on cloud.
**The database needed to be accessed by 100+ people.**
AWS Redshift can handle up to 500 connections. If this SSOT database will be accessed by 100+ people, we can move this database to Redshift with confidence to handle this request. Cost/Benefit analysis will be needed if we are going be implement this cloud solution.
