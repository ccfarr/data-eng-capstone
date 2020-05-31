from pyspark.sql.types import IntegerType, DoubleType
import pyspark.sql.functions as F
from ingest_data import create_spark_session
from pyspark.sql.dataframe import DataFrame
from pyspark.sql import SparkSession

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

def clean_countries_of_the_world(spark: SparkSession) -> DataFrame:
    """ Changes data types of selected columns and makes name column unique """

    # Filename of input file
    filename = '/Users/chrisfarr/data-eng-capstone/staging/countries_of_the_world.csv'

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

    # Return cleaned dataframe
    return df

def clean_i94_cit_res_data(spark: SparkSession, df_right: DataFrame) -> DataFrame:
    """ Changes data types of selected columns and makes name column unique """

    # Filename of input file
    filename = '/Users/chrisfarr/data-eng-capstone/staging/i94_cit_res_data.csv'

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
         .otherwise(df.country_join))

    # Return cleaned dataframe
    return df.join(df_right, df.country_join == df_right.country, how='left') \
            .select('country_id', df.country, df_right.country.alias('country_fk'))

def main():

    # Get Spark Session object
    spark = create_spark_session()

    # Clean staging/countires_of_the_world.csv 
    df_cow = clean_countries_of_the_world(spark)

    # Clean staging/i94_cit_res_data.csv
    df_cr = clean_i94_cit_res_data(spark, df_cow)
                 
    # df_cr.show(300, truncate=False)
    df_cr.filter(F.col('country_fk').isNull()).show(300, truncate=False)
    print(df_cr.count())

if __name__ == '__main__':
    main() 