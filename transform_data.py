from pyspark.sql import SparkSession
import pyspark.sql.functions as F

def create_spark_session() -> SparkSession:
    """ Creates and returns a SparkSession object"""

    spark = SparkSession \
        .builder \
        .config("spark.jars.packages","saurfang:spark-sas7bdat:2.0.0-s_2.11") \
        .getOrCreate()

    return spark

def main():

    # Get SparkSession object
    spark = create_spark_session()


if __name__ == '__main__':
    main() 