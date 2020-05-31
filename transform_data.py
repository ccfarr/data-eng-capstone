from pyspark.sql.types import IntegerType, DoubleType
import pyspark.sql.functions as F
from pyspark.sql.dataframe import DataFrame
from pyspark.sql import SparkSession

def create_spark_session() -> SparkSession:
    """ Creates and returns a SparkSession object"""

    spark = SparkSession \
        .builder \
        .getOrCreate()

    return spark

def clean_column_name(column_name: str) -> str:
    """ "Cleans" a column name by removing spaces, etc. """

    # Replace various characters and remove leading/trailing whitespace
    clean_name = column_name.replace('(','')    \
                            .replace(')','')    \
                            .replace('%','perc')    \
                            .replace('/','_')   \
                            .replace('.','')    \
                            .replace('$','dol') \
                            .lower()            \
                            .strip()            \
                            .replace(' ','_')

    # Return cleaned name
    return clean_name

def transform_countries_of_the_world(spark: SparkSession) -> DataFrame:
    """ Changes data types of selected columns and makes name column unique """

    # Filename of input file
    filename = 's3://data-eng-capstone-cf/staging/countries_of_the_world.csv'

    # Read into a spark dataframe
    df = spark.read.csv(filename, header=True)

    # Clean column names, get rid of space, comma, parenthesis, etc.
    for c in df.columns:
        df = df.withColumnRenamed(c, clean_column_name(c))

    # Change data types
    # Country and Region columns are truly strings
    # Population, Area (sq. mi.), GDP ($ per capita) and Climate are whole numbers
    int_cols = ['population', 'area_sq_mi', 'climate']
    for c in int_cols:
        df = df.withColumn(c, F.col(c).cast(IntegerType()))

    # All other columns are decimals
    # Make decimal separator a period versus comma (US format)
    for c in df.columns:
        if c not in int_cols + ['country', 'region']:
            df = df.withColumn(c, F.regexp_replace(c,',','.').cast(DoubleType()))

    # Remove trailing and leading white space from Country and Region
    for c in ['country', 'region']:
        df = df.withColumn(c, F.trim(F.col(c)))

    # Return transformed dataframe
    return df

def transform_i94_cit_res_data(spark: SparkSession, df_right: DataFrame) -> DataFrame:
    """ Changes data types of selected columns and makes name column unique """

    # Filename of input file
    filename = 's3://data-eng-capstone-cf/staging/i94_cit_res_data.csv'

    # Read into a spark dataframe
    df = spark.read.csv(filename, header=True)

    # Cast country_id as an IntegerType()
    df = df.withColumn('country_id', df.country_id.cast(IntegerType()))

    # Make country unique, by appending '(<country_id>)' to string name
    # Only do when country equals INVALID: STATELESS or INVALID: UNITED STATES
    df = df.withColumn('country',
                        F.when(df.country.isin('INVALID: STATELESS', 'INVALID: UNITED STATES'),
                               F.concat(df.country, F.lit(' ('), df.country_id, F.lit(')')))
                         .otherwise(df.country))

    # Add foreign key column so can join to df_cow
    df = df.withColumn('country_join', F.initcap('country'))

    # Manual adjustments
    df = df.withColumn('country_join',
        F.when(df.country == 'MEXICO Air Sea, and Not Reported (I-94, no land arrivals)', 'Mexico')
         .when(df.country == 'ANTIGUA-BARBUDA', 'Antigua & Barbuda')
         .when(df.country == 'BAHAMAS', 'Bahamas, The')
         .when(df.country == 'BOSNIA-HERZEGOVINA', 'Bosnia & Herzegovina')
         .when(df.country == 'BRITISH VIRGIN ISLANDS', 'British Virgin Is.')
         .when(df.country == 'CENTRAL AFRICAN REPUBLIC', 'Central African Rep.')
         .when(df.country == 'GAMBIA', 'Gambia, The')
         .when(df.country == 'GUINEA-BISSAU', 'Guinea-Bissau')
         .when(df.country == 'MAYOTTE (AFRICA - FRENCH)', 'Mayotte')
         .when(df.country == 'MICRONESIA, FED. STATES OF', 'Micronesia, Fed. St.')
         .when(df.country == 'NORTH KOREA', 'Korea, North')
         .when(df.country == 'MICRONESIA, FED. STATES OF', 'Micronesia, Fed. St.')
         .when(df.country == 'MICRONESIA, FED. STATES OF', 'Micronesia, Fed. St.')
         .when(df.country == 'SOUTH KOREA', 'Korea, South')
         .when(df.country == 'ST. HELENA', 'Saint Helena')
         .when(df.country == 'ST. KITTS-NEVIS', 'Saint Kitts & Nevis')
         .when(df.country == 'ST. LUCIA', 'Saint Lucia')
         .when(df.country == 'ST. PIERRE AND MIQUELON', 'St Pierre & Miquelon')
         .when(df.country == 'ST. VINCENT-GRENADINES', 'Saint Vincent and the Grenadines')
         .when(df.country == 'TRINIDAD AND TOBAGO', 'Trinidad & Tobago')
         .when(df.country == 'TURKS AND CAICOS ISLANDS', 'Turks & Caicos Is')
         .when(df.country == 'WALLIS AND FUTUNA ISLANDS', 'Wallis and Futuna')
         .when(df.country == 'CHINA, PRC', 'China')
         .otherwise(df.country_join))

    # Define country_fk via left outer join
    df = df.join(df_right, df.country_join == df_right.country, how='left') \
           .select('country_id', df.country, df_right.country.alias('country_fk'))

    # Return transformed dataframe
    return df

def transform_i94_data(spark: SparkSession, df_right: DataFrame) -> DataFrame:
    """ Transforms the i9_data as outlined below """

    # Filename of input file
    filename = 's3://data-eng-capstone-cf/staging/i94_data'

    # Read into a spark dataframe
    df = spark.read.parquet(filename)

    # Keep columns of interest (i94mon, i94res, i94mode, i94bir, i94visa, visatype)
    cols = ['i94mon', 'i94res', 'i94mode', 'i94bir', 'i94visa', 'visatype']
    df = df.select(cols)

    # Cast double typed columns as ints (i94mon, i94res, i94mode, i94bir, i94visa)
    cols.remove('visatype')
    for col in cols:
        df = df.withColumn(col, F.col(col).cast(IntegerType()))

    # Decode selected columns using mapping file (i94res)
    # Include country_fk column from mapping file

    # Define country_fk via left outer join
    df = df.join(df_right, df.i94res == df_right.country_id, how='left')
    df = df.drop('country_id')

    # Decode selected columns using F.when (i94mode, i94visa)
    # i94mode
    df = df.withColumn('i94mode_desc',
            F.when(df.i94mode == 1, 'Air')
             .when(df.i94mode == 2, 'Sea')
             .when(df.i94mode == 3, 'Land')
             .when(df.i94mode == 9, 'Not reported')
             .otherwise('Other'))
    df = df.drop('i94mode')
    df = df.withColumnRenamed('i94mode_desc', 'i94mode')

    # i94visa
    df = df.withColumn('i94visa_desc',
            F.when(df.i94visa == 1, 'Business')
             .when(df.i94visa == 2, 'Pleasure')
             .when(df.i94visa == 3, 'Student')
             .otherwise('Other'))
    df = df.drop('i94visa')
    df = df.withColumnRenamed('i94visa_desc', 'i94visa')

    # Return transformed dataframe
    return df

def main():

    # Get Spark Session object
    spark = create_spark_session()

    # Transform staging/countires_of_the_world.csv 
    df_cow = transform_countries_of_the_world(spark)

    # Transform staging/i94_cit_res_data.csv
    df_cr = transform_i94_cit_res_data(spark, df_cow)

    # Transform staging/i94_data.parquet
    df_i94 = transform_i94_data(spark, df_cr)

    df = df_i94.filter(~ df_i94.country.contains('MEXICO')) \
               .groupBy('country', 'country_fk', 'i94mon') \
               .count()

    df = df.repartition(1)

    df.write.csv(path='s3://data-eng-capstone-cf/production/analysis.csv',
                 mode='overwrite',
                 header=True)

    # print(f'*** IT WORKED *** There are {df_i94.count()} columns in staging/i94_data.parquet.')

if __name__ == '__main__':
    main() 