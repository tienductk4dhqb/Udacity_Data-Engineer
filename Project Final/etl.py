"""Run file ETL."""
import configparser
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

config = configparser.ConfigParser()
config.read_file(open('aws.cfg'))

os.environ["AWS_ACCESS_KEY_ID"] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ["AWS_SECRET_ACCESS_KEY"] = config['AWS']['AWS_SECRET_ACCESS_KEY']
output_data = config['AWS']['OUTPUT_DATA']


def create_spark_session():
    """Create a Apache Spark session to process the data.
    
    Keyword arguments:
    * N/A
    Output:
    * spark -- An Apache Spark session.
    """
    
    spark = SparkSession.builder\
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0")\
        .config("spark.debug.maxToStringFields", "1000")\
        .enableHiveSupport()\
        .getOrCreate()
    return spark


def process_immigration_data(spark, output_data):
    """Read and write file from folder sas_data, 
    store in S3 directory(output_data).
    
    process the data to extract:
    - fact_immigration
    - dimension_personal
    - dimension_airline
    Keyword arguments:
    * spark            -- reference to Spark session.
    * output_data      -- path to location to store the output
                          (parquet files).
    Output:
    * fact_immigration   -- directory with fact_immigration parquet files
                          stored in output_data path.
    * dimension_personal      -- directory with dimension_personal parquet files
                          stored in output_data path.
    * dimension_airline   -- directory with dimension_airline parquet files
                          stored in output_data path.
    """
    
    output_immigration = output_data+"/Immigration"
    # Read file sas
    print("Start read and write immigration")
    df = spark.read.parquet("sas_data")
    df.createOrReplaceTempView("fact_immigration")
    # Immigration fact
    print("Start write fact_immigration")
    fact_immigration = spark.sql("""
        SELECT
            cicid as cic_id,
            i94yr as year,
            i94mon as month,
            i94cit as cit,
            i94res as res,
            i94port as port,
            arrdate as arr_date,
            i94mode as mode,
            i94addr as address,
            depdate as dep_date,
            i94bir as birth_day,
            i94visa as visa,
            count as total,
            entdepd,
            matflag,
            biryear as birth_year,
            dtaddto,
            gender,
            airline,
            admnum,
            fltno,
            visatype
        FROM fact_immigration
    """)
    fact_immigration.write.mode("overwrite")\
        .parquet(output_immigration+"/Fact/immigration.parquet")
    print("Finish write fact_immigration")
    
    # Immigration dimension
    # dimension_personal
    print("Start write dimension_personal")
    dimension_personal = fact_immigration\
        .select('cic_id', 'year', 'month', 'address',
                'birth_day', 'birth_year', 'gender'
               )\
        .distinct() 
    dimension_personal.write.mode("overwrite")\
        .parquet(output_immigration+"/Dimension/personal.parquet")
    print("Finish write dimension_personal")
    
    # dimension_airline
    print("Start write dimension_airline")
    dimension_airline = fact_immigration\
        .select('cic_id', 'port', 'airline', 'visa', 'visatype', 'dep_date')\
        .distinct()
    dimension_airline.write.mode("overwrite")\
        .parquet(output_immigration+"/Dimension/airline.parquet")
    print("Finish write dimension_airline")
    print("Finish read and write immigration")
    
    
def process_temperature_data(spark, output_data):
    """Read and write file from folder GlobalLandTemperaturesByCity.csv, 
    store in S3 directory(output_data).
    
    process the data to extract:
    - fact_temperature
    Keyword arguments:
    * spark            -- reference to Spark session.
    * output_data      -- path to location to store the output
                          (parquet files).
    Output:
    * fact_temperature   -- directory with fact_immigration parquet files
                          stored in output_data path.
    """
    
    output_temperature = output_data+"/Temperature"
    # Read file GlobalLandTemperaturesByCity
    print("Start read and write temperature")
    temperature_path = '../../data2/GlobalLandTemperaturesByCity.csv'
    df = spark.read.csv(temperature_path, header = True)
    fact_temperature = df.select('dt',
                            col('AverageTemperature').alias('avg_temperature'),
                            col('AverageTemperatureUncertainty').alias('avg_uncertainty'),
                            col('City').alias('city'),
                            col('Country').alias('country'),
                            col('Latitude').alias('latitude'),
                            col('Longitude').alias('longitude')
                            ).withColumn("id", monotonically_increasing_id())
    fact_temperature.write.mode("overwrite").parquet(output_temperature+"/temperature.parquet")
    print("Finish read and write temperature")
                                                 

def process_demographic_data(spark, output_data):
    """Read and write file from folder us-cities-demographics.csv, 
    store in S3 directory(output_data).
    
    process the data to extract:
    - fact_demographic
    - dimension_population
    - dimention_city_statistics
    Keyword arguments:
    * spark            -- reference to Spark session.
    * output_data      -- path to location to store the output
                          (parquet files).
    Output:
    * fact_immigration   -- directory with fact_immigration parquet files
                          stored in output_data path.
    * dimension_personal      -- directory with dimension_personal parquet files
                          stored in output_data path.
    * dimension_airline   -- directory with dimension_airline parquet files
                          stored in output_data path.
    """
    
    output_demographic = output_data+"/Demographic"
    df = spark.read.option("delimiter", ";").csv("us-cities-demographics.csv", header = True)
    df = df.select("*").withColumn("id", monotonically_increasing_id())
    
    print("Start read and write fact and dimension demographic")
    print("Start read and write fact_demographic")    
    df.createOrReplaceTempView("fact_demographic_df")
    fact_demographic = spark.sql("""
        SELECT
            id,
            City as city,
            State as state,
            'Median Age' as median_age,
            'Male Population' as male_population,
            'Female Population' as female_population,
            'Total Population' as total_population,
            'Number of Veterans' as number_of_veterans,
            'Foreign-born' as foreign_born,
            'Average Household Size' as avg_house_hold_size,
            'State Code' as stage_code,
            Race as race,
            Count as total
         FROM fact_demographic_df
     """)
    fact_demographic.write.mode("overwrite").parquet(output_demographic+"/Fact/demographic.parquet")
    print("Finish read and write fact_demographic")
    # Dimension demographic
    # dimension_population
    print("Start read and write dimension_population")
    dimension_population = fact_demographic.select('id', 'city', 'state', 'male_population',
                                                   'female_population', 'number_of_veterans',
                                                   'foreign_born', 'race'
                                                  )
    dimension_population.write.mode("overwrite")\
        .parquet(output_demographic+"/Dimension/population.parquet")
    print("Finish read and write dimension_population")
    # dimention_city_statistics    
    print("Start read and write dimention_city_statistics")
    dimention_city_statistics = fact_demographic.select('id', 'city', 'state', 'stage_code',
                                                        'total_population', 'avg_house_hold_size'
                                                       )
    dimention_city_statistics.write.mode("overwrite")\
        .parquet(output_demographic+"/Dimension/city_statistic.parquet")
    print("Finish read and write dimention_city_statistics")    
    
    
def process_airport_data(spark, output_data):
    """Read and write file from folder us-cities-demographics, 
    store in S3 directory(output_data).
    
    process the data to extract:
    - fact_airport
    Keyword arguments:
    * spark            -- reference to Spark session.
    * output_data      -- path to location to store the output
                          (parquet files).
    Output:
    * fact_airport   -- directory with fact_immigration parquet files
                          stored in output_data path.
    """

    output_airport = output_data+"/Airport"
    print("Start read and write airport")
    df = spark.read.csv("airport-codes_csv.csv" , header = True)
    fact_airport = df.withColumn("id", monotonically_increasing_id())
    fact_airport.write.mode("overwrite").parquet(output_airport+"/airport.parquet")
    print("Finish read and write airport")
    
    
def process_label_description_data(spark, output_data):
    """Read and write file I94_SAS_Labels_Descriptions, 
    store in S3 directory(output_data).
    
    process the data to extract:
    - dimension_country
    - dimention_mode
    - dimention_airport
    - dimension_stage
    Keyword arguments:
    * spark            -- reference to Spark session.
    * output_data      -- path to location to store the output
                          (parquet files).
    Output:
    * dimension_country   -- directory with dimension_country parquet files
                          stored in output_data path.
    * dimention_mode      -- directory with dimention_mode parquet files
                          stored in output_data path.
    * dimention_airport   -- directory with dimention_airport parquet files
                          stored in output_data path.
    * dimension_stage     -- directory with dimension_stage parquet files
                          stored in output_data path.
    """    
    
    # Read file sas
    with open("I94_SAS_Labels_Descriptions.SAS") as f:
        contents = f.readlines()
    output_label = output_data+"/Label Description"
    # Country
    print("Start read and write dimension_country")
    # 10:274 is range valid values country
    country_data = {}    
    for countries in contents[10:274]:
        pair = countries.split('=')
        country_code = pair[0].strip()
        country_name = pair[1].strip().strip("'")
        country_data[country_code] = country_name
    spark.createDataFrame(country_data.items(), ['code', 'country'])\
        .write.mode("overwrite")\
        .parquet(output_label + '/country.parquet')
    print("Finish read and write dimension_country") 
    
    # Airport
    print("Start read and write dimension_airport")
    # 302:894 is range valid for airport
    airport_data = {}
    for airports in contents[302:894]:
        pair = airports.split('=')
        airport_code = pair[0].strip().strip("\t").strip("'")
        airport_name = pair[1].strip().strip("'")
        airport_data[airport_code] = airport_name
    spark.createDataFrame(airport_data.items(), ['code', 'airport'])\
        .write.mode("overwrite")\
        .parquet(output_label + '/airport.parquet')
    print("Finish read write dimension_airport")
    
    # Stage
    print("Start read and write dimension_stage")
    # 982:1036 is range valid for stage
    stage_data = {}
    for stages in contents[982:1036]:
        pair = stages.split('=')
        stage_code = pair[0].strip().strip("\t").strip("'")
        stage_name = pair[1].strip().strip("'")
        stage_data[stage_code] = stage_name
    spark.createDataFrame(stage_data.items(), ['code', 'stage'])\
        .write.mode("overwrite")\
        .parquet(output_label + '/stage.parquet')
    print("Finish read write dimension_stage")
    
    # Mode
    print("Start read and write dimension_mode")
    # 972:975 is range valid for mode
    mode_data = {}
    for citys in contents[972:975]:
        pair = citys.split('=')
        mode_code = pair[0].strip().strip("\t").strip("'")
        mode_name = pair[1].strip().strip("'")
        mode_data[mode_code] = mode_name
    spark.createDataFrame(mode_data.items(), ['code', 'mode'])\
        .write.mode("overwrite")\
        .parquet(output_label + '/mode.parquet')
    print("Finish read write dimension_mode")
    

def main():
    """Processing process.
    
    process the data to extract
    fact_immigration
    dimension_personal
    dimension_airline
    ----
    fact_temperature
    ----
    fact_demographic
    dimension_population
    dimention_city_statistics
    ----
    fact_airport
    dimension_country
    dimention_mode
    dimention_airport
    dimension_stage
    and store data to parquet files to output_data path.
    Keyword arguments:
    * NA
    Output:    
    """
    
    spark = create_spark_session()
    # Use path input from local
    output_data = config['AWS']['OUTPUT_DATA']
    
    # Process Immigration
#     process_immigration_data(spark, output_data)
#     # Process Temperature
#     process_temperature_data(spark, output_data)
#     # Process Demographic
#     process_demographic_data(spark, output_data)
#     # Process Airport
#     process_airport_data(spark, output_data)
    # Process Label Description
    process_label_description_data(spark, output_data)


if __name__ == "__main__":
    main()
