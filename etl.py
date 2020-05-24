from pyspark.sql import SparkSession, DataFrame
import glob
from typing import Tuple
import pyspark.sql.functions as F
from itertools import chain
import pandas as pd

def create_spark_session() -> SparkSession:
    """ Creates and returns a SparkSession object"""

    spark = SparkSession \
        .builder \
        .config("spark.jars.packages","saurfang:spark-sas7bdat:2.0.0-s_2.11") \
        .getOrCreate()

    return spark

def extract_i94_data(spark: SparkSession) -> DataFrame:
    """
    Extracts i94 data for the year 2016 from 12, monthly SAS files
    Returns a Spark DataFrame
    """

    # Read the January file
    df = spark.read.format('com.github.saurfang.sas.spark') \
                .load('./data/18-83510-I94-Data-2016/i94_jan16_sub.sas7bdat')

    # Define  a list of the remaining files
    files = glob.glob('./data/18-83510-I94-Data-2016/' + '*.sas7bdat')
    files_notjan = [file for file in files if ("jan16" not in file)]

    # Loop over this list to create one consolidated dataframe for 2016
    for file in files_notjan:
        # Read one of the monthly files
        df_new = spark.read.format('com.github.saurfang.sas.spark').load(file)

        # Delete extra columns from June 2016
        if 'jun16' in file:
            df_new = df_new.drop('delete_days')
            df_new = df_new.drop('delete_mexl')
            df_new = df_new.drop('delete_dup')
            df_new = df_new.drop('delete_visa')
            df_new = df_new.drop('delete_recdup')
            df_new = df_new.drop('validres')

        # Union together
        df = df.union(df_new)

    # Check schema
    print(df.printSchema())

    # Return DataFrame
    return df

def extract_mapping() -> dict:
    """
    Extracts mapping for Country of Citizenship/Residence
    """
    # Open SAS meta file
    file = open(r'./data/I94_SAS_Labels_Descriptions.SAS', 'r')
    
    # Read in
    lines = file.readlines()
    
    # Close file
    file.close()

    # Subset lines of interest
    countries = lines[9:298]

    # Define empty dict
    mapping = {}

    # Add parsed country info to dictionary
    for country in countries:
        key, value = parse_row(country)
        mapping[key] = value

    return mapping

def parse_row(row:str) -> Tuple:
    """Parses row into a key, value pair"""
    
    # Parse into a list, removing ' and ;
    row_list = row.strip() \
                  .replace('\'','') \
                  .replace(';','') \
                  .split('=')
    
    # Get key and value, removing any white space
    key = float(row_list[0].strip())
    value = row_list[1].strip()

    # Return key/value tuple
    return (key, value)

def main():

    # Get SparkSession object
    spark = create_spark_session()

    # Extract i94 data from SAS files
    df = extract_i94_data(spark)

    # Create Country of Citizenship/Residence dictionary
    mapping = extract_mapping()

    # Build mapping_expr to decode i94cit and i94res
    mapping_expr = F.create_map([F.lit(x) for x in chain(*mapping.items())])

    # Decode i94cit and i94res using mapping_expr
    df = df.withColumn("i94cit_desc", mapping_expr.getItem(F.col('i94cit')))
    df = df.withColumn("i94res_desc", mapping_expr.getItem(F.col('i94res')))

    # I-94 Arrivals by Country of Residence (COR) and Month
    # Exludes Mexico to focus on Overseas countries
    df.filter(~ df.i94res_desc.contains('MEXICO')) \
      .groupBy('i94res_desc', 'i94mon') \
      .count() \
      .toPandas() \
      .to_csv('out.csv', index=False)

if __name__ == '__main__':
    main() 