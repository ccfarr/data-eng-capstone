from pyspark.sql import SparkSession, DataFrame
import glob

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

def main():

    # Get SparkSession object
    spark = create_spark_session()

    # Extract i94 data from SAS files
    df = extract_i94_data(spark)

if __name__ == '__main__':
    main() 