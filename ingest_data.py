from pyspark.sql import SparkSession
import glob
import pyspark.sql.functions as F
from typing import Tuple
import csv

def create_spark_session() -> SparkSession:
    """ Creates and returns a SparkSession object"""

    spark = SparkSession \
        .builder \
        .config("spark.jars.packages","saurfang:spark-sas7bdat:2.0.0-s_2.11") \
        .getOrCreate()

    return spark

def ingest_i94_data():
    """
    Ingests i94 data for the year 2016 from 12, monthly SAS files
    """

    # Get Spark Session object
    spark = spark.create_spark_session()

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
            df_new = df_new.drop('delete_days')   \
                           .drop('delete_mexl')   \
                           .drop('delete_dup')    \
                           .drop('delete_visa')   \
                           .drop('delete_recdup') \
                           .drop('validres')

        # Union together
        df = df.union(df_new)

        # Write to parquet file for later processing
        df.write.parquet('./staging/i94_data', mode='overwrite')

def parse_row(row:str) -> Tuple:
    """Parses row into a key, value pair"""
    
    # Parse into a list, removing ' and ;
    row_list = row.strip() \
                  .replace('\'','') \
                  .replace(';','') \
                  .split('=')
    
    # Get key and value, removing any white space
    key = row_list[0].strip()
    value = row_list[1].strip()

    # Return key/value tuple
    return (key, value)

def ingest_coc_cor_data():
    """
    Ingests decode mapping for Country of Citizenship/Residence
    """
    # Open SAS meta file
    with open('./data/I94_SAS_Labels_Descriptions.SAS', mode='r') as infile:
        # Read in
        lines = infile.readlines()
    
    # Subset lines of interest
    countries = lines[9:298]

    # Write to CSV file
    with open('./staging/data_coc_cor.csv', mode='w') as outfile:
        # Assign writer object
        outfile_writer = csv.writer(outfile)

        # Loop over countries, writing out tuple
        for country in countries:
            outfile_writer.writerow(parse_row(country))

def main():

    # Ingest i94 data from SAS files
    # ingest_i94_data()

    # Ingest Country of Citizenship/Residence Mapping
    ingest_coc_cor_data()

if __name__ == '__main__':
    main() 