# data-eng-capstone
**Data Engineering Capstone Project**  
**Chris Farr, PhD**  
**June 2020**  

### Step 1: Scope the Project and Gather Data

#### Scope 
The following diagram provides an overview of the scope of my project.

![Overview](./images/Overview.png?raw=true)

In summary, I will take raw data captured from i94 forms in the year of 2016 (12 monthly files) and integrate it with country-level attributes, like population and GDP per capita, obtained from a "Countries of the World" csv file obtained from the website Kaggle ([source](https://www.kaggle.com/fernandol/countries-of-the-world)).

The monthly, i94 files are large, ranging in size from 391 MB to 716 MB, for a total of 6.51 GB on disk for all 12 files. (All 12 files combined contain 40,790,529 records.) Given the size of this data, I'll be using PySpark and EMR instances to process. The final deliverable will be two parquet files (a fact table and a dimension table) hosted on Amazon's S3 storage platform. I will author a EMR notebook to illustrate how these tables can be joined together and queried using the PySpark DataFrame API.

#### Describe and Gather Data 
I used two primary datasets:

**i94 data**

The i94 data contains information about visitors to the US via an i94 form that all visitors must complete. I acquired the data as follows:

From a terminal in Udacity's JupyterLab environment, I compressed the I-94 data by typing the following from the default prompt (`/home/workspace`):

```
tar -zcvf data.tar.gz ../../data
```
z - use gzip  
c - create new archive  
v - verbose  
f - use given archive file  

After downloading `data.tar.gz` to my local machine, I then uncompressed the files using my Mac's built-in Archive Utility. The result was 12 files:

```
i94_jan16_sub.sas7bdat
i94_feb16_sub.sas7bdat
i94_mar16_sub.sas7bdat
i94_apr16_sub.sas7bdat
i94_may16_sub.sas7bdat
i94_jun16_sub.sas7bdat  
i94_jul16_sub.sas7bdat
i94_aug16_sub.sas7bdat
i94_sep16_sub.sas7bdat
i94_oct16_sub.sas7bdat
i94_nov16_sub.sas7bdat
i94_dec16_sub.sas7bdat
```

I also downloaded a small "mapping" file (i94_SAS_Labels_Descriptions.SAS) used to decode numeric values.

**Countries of the World data**

This data contains information about countries like population, GDP per capita and other country-level attributes.

I downloaded this CSV file from Kaggle.com ([source](https://www.kaggle.com/fernandol/countries-of-the-world)).

### Step 2: Explore and Assess the Data

#### Explore the Data 

I explored the three datasets that reside in a staging directory on S3 using EMR notebooks.

**staging/countries_of_the_world.csv**

See [notebooks/explore_countries_of_the_world.ipynb](notebooks/explore_countries_of_the_world.ipynb)

* 227 records in total
* 20 columns, all read in as strings
 * Country and Region columns are truly strings
 * Population, Area (sq. mi.), GDP ($ per capita) and Climate are whole numbers
 * All other columns are decimals
* Country, the primary key, does not contain any duplicate values or missing values
* Descriptive statistics (e.g. China most populous) as expected

**staging/i94_cit_res_data.csv**

See [notebooks/explore_i94_cit_res_data.ipynb](notebooks/explore_i94_cit_res_data.ipynb)

* 289 records
* 2 columns: country_id and country
* Both columns come in as strings even though country_id is really numeric
* Neither column contains any missing values
* country_id has no duplicates but country does

**staging/i94_data.parquet**

See [notebooks/explore_i94_data.ipynb](notebooks/explore_i94_data.ipynb)

* 40,790,529 records
* 28 columns
* Numeric columns all typed as doubles
* i94res has no missing values
* i94bir has suspicious values, e.g. age < 0 and age >> 100

Note: The python script [ingest_data.py](ingest_data.py) unions the 12 monthly files to produce this dataset. While performing the union, I noticed that the June file had extra columns, which I had to remove. See the python script for more details.

#### Cleaning Steps
I identified the following cleaning steps, which I implemented in [process_data.py](process_data.py):

**staging/countries_of_the_world.csv**

* Clean column names: remove space, comma, parenthesis, etc.
* Select subset of columns:
  * country, region, population, and gdp_dol_per_capita
* Change data types
  * country and region are already strings, leave as is
  * population and gdp_dol_per_capita to ints
* Remove trailing and leading white space from country and region

**staging/i94_cit_res_data.csv**

* Cast country_id as int
* Make country unique, by appending `(<country_id>)` to string name
  * Only do when country equals INVALID: STATELESS or INVALID: UNITED STATES
* Add foreign key column so can join to df_cow, name country_fk

**staging/i94_data.parquet**

* Keep columns of interest and aggregate
  * i94mon, i94res, i94mode, i94bir, i94visa, visatype
* Cast double typed columns to ints:
  * i94mon, i94res, i94mode, i94bir, i94visa
* Decode selected columns using mapping file (i94res)
* Decode selected columns using F.when (i94mode, i94visa)

### Step 3: Define the Data Model

#### 3.1 Conceptual Data Model
I thought a simple, two table star schema was sufficient for this project. A fact table contains the events, which in this case is a person's visit. The data will be aggregated to the dimensions of interest (see data dictionary below). The second table in the schema is a dimension table for the countries sourced from the "Countries of the World" dataset. The `country` column in this table can be joined using the `country_fk` column in the fact table.

#### 3.2 Mapping Out Data Pipelines
As shown in [process_data.py](process_data.py), the main steps are as follows:

1. Process `staging/countires_of_the_world.csv` (applying the transformations described above), yielding `production/dim_countires_of_the_world.parquet`.

2. Prepare `staging/i94_cit_res_data.csv`, which will be used to both decode the numeric country identifier and add the `country_fk` column in the i94 data.

3. Lastly, process `staging/i94_data.parquet`, yielding `production/fact_i94.parquet`.

### Step 4: Run Pipelines to Model the Data

#### 4.1 Create the data model
As shown in the overivew diagram, [ingest_data.py](ingest_data.py) converts the raw data into an intermediate state. Once here, the data is then transfromed and loaded by [process_data.py](process_data.py) into a production folder, ready for analysis.

#### 4.2 Data Quality Checks
In addition to checking if the record number counts were unchanged at various stages of the process, I compared the number of visitors in 2016 from a report on the government's official [website](https://travel.trade.gov/view/m-2017-I-001/index.asp) to a dataframe I generated using the following command:

```
# I-94 Arrivals by Country of Residence (COR) and Month
# Exludes Mexico to focus on Overseas countries
df_analysis = df_fact.filter(~df_fact.i94res.contains('MEXICO')) \
                     .groupBy('i94res') \
                     .sum('visitor_count') \
                     .orderBy(F.desc('sum(visitor_count)'))
```

The comparison is shown below, where the result of the dataframe is on the left and the figures from the government report is shown at right. A perfect match for the top 20 countries!

![QC Comparison](./images/QC%20Comparison.png?raw=true)

#### 4.3 Data dictionary 

**production/dim_countries_of_the_world**

All fields sourced from countries_of_the_world.csv ([link](https://www.kaggle.com/fernandol/countries-of-the-world))

| Field      | Description | Example Value | Data Type |
| ---------- | ----------- | ------------- | --------- |
| country | The country's name | Aruba | string |
| region | The country's geographic region | LATIN AMER. & CARIB | string |
| population | The country's population | 71891 | integer |
| gdp_dol_per_capita | The country's per capita GDP in US dollars | 28000 | integer |

**production/fact_i94**

All fields (expect for `country_fk`) sourced from i94 SAS files from Udacity.

| Field      | Description | Example Value | Data Type |
| ---------- | ----------- | ------------- | --------- |
| i94bir | The age of the visitors | 56 | integer |
| visatype | The visa type of the visitors | WT | string |
| visitor_count | The number of visitors | 1091 | long |
| country_fk | The country's corresponding name in the dimension table | Korea, South | string |
| i94mode | The mode of travel | Air | string |
| i94visa | The visa's purpose | Pleasure | string |
| i94mon | The month of arrival (2016) | 4 | integer |
| i94res | The country of residence of the visitors | SOUTH KOREA | string |

#### Step 5: Complete Project Write Up
As mentioned above, "big data" tools were needed to process this data given its size. Specifically, I used the following tools in my project:

* **PySpark** - PySpark allows you to interact with the Apache Spark data processing framework by writing Python code. In paricular, PySpark allows you to express data as "DataFrames," which allows you to concentrate on data transformations and other tasks without managing how the dataset is ditributed over nodes in the computing cluster.

* **EMR Cluster** - I ran my PySpark script [process_data.py](process_data.py) on a cluster of computers (1 Master and 2 Core nodes) provided by Amazon's EMR Cluster service. I simply had to SSH into the cluster and issue `spark-submit process_data.py` to kick off the spark job. Details on setup and how to run spark jobs given below.

* **S3** - I created staging and production folders on Amazon's S3 storage service. The EMR cluster I used to process my PySpark script and EMR notebooks accessed the data directly from S3 (versus me loading the data myself into the cluster).

* **EMR Notebooks** - I used Jupyter notebooks to both explore the data and analyze the data once put into a star-schema. Specifically, I used Amazon's EMR notebooks, which you attach to a running cluster. I found that opening the notebook in a JupyterLab environment allowed me to upload and download workbooks easily.

* **PySpark in Local Mode** - After downloading the raw data to my local machine, I did do some processing locally after I setup PySpark (details below). [ingest_data.py](ingest_data.py) not only processed the data locally but uploaded the data to a staging folder on S3. All of the remaining processing was done on an EMR Cluster.

The production data could certainly be updated on a more frequent basis, per the release schedule by the relevant governmental authority. The tools used in this project could also handle data at a larger scale, say 100x. The number of nodes configured in the EMR cluster would just need to increase as needed, of course. I also would need to sort out how to effectively extract the data into a staging environment, versus relying on my laptop. If data needs to be updated on a daily basis, a scheduling framework like Airflow could be used to schedule data processing "runs" and signal if any failures in the process. Lastly, if the end deliverable needs to support a dashboard viewed by 100 plus people, the production data could be loaded into a data warehousing technology like Amazon Redshift, which could then support a wide array of BI tools.

### Setup and Working with EMR cluster on AWS

#### EMR Cluster Configuration

![EMR Cluster Configuration](./images/EMR%20Cluster%20Configuration.png?raw=true)

* When using release emr-5.30.0, I received `Failed to start the kernel` error message when starting an EMR notebook. Had success after downgrading to release emr-5.29.0. ([link](https://stackoverflow.com/questions/61951352/notebooks-on-emr-aws-failed-to-start-kernel))  

* Switched to us-east-1 region after getting `The requested instance type m5.xlarge is not supported in the requested availability zone.`  

#### Setting up SSH

##### Generate Key Pair and associate it with EMR cluster

1. Go to the Amazon EC2 console ([link](https://us-west-2.console.aws.amazon.com/ec2/v2/home))  
2. In the Navigation pane, click Key Pairs
3. On the Key Pairs page, click Create Key Pair
4. In the Create Key Pair dialog box, enter a name for your key pair, such as, mykeypair
5. Click Create
6. Save the resulting PEM file in a safe location
7. Associate this Key Pair when initializing EMR cluster

##### Run the following at command prompt 

Download the Key Pair and run `chmod og-rwx mykeypair.pem` at command prompt

##### Add inbound rule to the security group of the master node

1. Click security groups for master
2. Click the relevant Group ID
3. Click Inbound tab at bottom
4. Add rule with the following properties:
    Type: SSH
    Protocol: TCP
    Port Range: 22
    Source: 0.0.0.0/0

#### Using SSH to run process_data.py

1. To establish ssh session (from local machine): `ssh -i mykeypair.pem hadoop@ec2-###-##-##-###.compute-1.amazonaws.com`

2. To use python3 interpreter (from remote machine): `sudo sed -i -e '$a\export PYSPARK_PYTHON=/usr/bin/python3' /etc/spark/conf/spark-env.sh`  

3. To copy files (from local machine): `scp -i mykeypair.pem process_data.py hadoop@ec2-###-##-##-###.compute-1.amazonaws.com:/home/hadoop`

4. To run python file (from remote machine): `spark-submit process_data.py`

### Setup on Local Computer (ingest_data.py)

I ran development versions of my PySpark code in Local Mode on my Mac which has the following hardware configuration:

* MacBook Pro (Retina, 13-inch, Early 2013)
* OS: macOS Catalina, Version 10.15.4
* Processor: 3 GHz Dual-Core Intel Core i7
* Memory: 8 GB 1600 MHz DDR3
* HD: 500 GB Flash Storage (432.1 GB Available)

#### Installing Java

According to Spark's documentation:

> Spark runs on both Windows and UNIX-like systems (e.g. Linux, Mac OS). It’s easy to run locally on one machine — all you need is to have java installed on your system PATH, or the JAVA_HOME environment variable pointing to a Java installation.

I installed Java's runtime engine (JRE) first via this [link](https://java.com/en/download/mac_download.jsp). It installed Version 8 Update 251. It turns out I needed Java's development kit (JDK) instead. I first installed version 11 of the JDK but received ominous warning messages like the following: 

```
WARNING: An illegal reflective access operation has occurred
```

I removed Version 11 (by deleting the version 11 folder from `/Library/Java/JavaVirtualMachines`) and installed version 8.

```
% java -version
java version "1.8.0_251"
Java(TM) SE Runtime Environment (build 1.8.0_251-b08)
Java HotSpot(TM) 64-Bit Server VM (build 25.251-b08, mixed mode)
```

The JDK is located on my file system here: `/Library/Java/JavaVirtualMachines/jdk1.8.0_251.jdk`.

#### Virtual Environment

I already have Anaconda installed and created a virtual environment for my project.

```
conda create -n spark python=3.7.4
```

To activate this from a command line type `conda activate spark`.

#### PySpark

I downloaded `spark-2.4.5-bin-hadoop2.7` but not sure if I needed this ultimately. I just pip installed the PySpark library as so:

```
python -m pip install pyspark
```

I also installed three additional libraries

```
python -m pip install pylint
python -m pip install jupyter
python -m pip install pandas
```

### Useful Resources

[Parsing Text Files in Python](https://www.google.com/search?q=parse+text+file+python&oq=parse+text+file+python&aqs=chrome..69i57j0l7.5567j1j7&sourceid=chrome&ie=UTF-8#kpvalbx=_f2jJXt3WJIbXtAaopaPIDw33)

[Gitignore Explained](https://www.freecodecamp.org/news/gitignore-what-is-it-and-how-to-add-to-repo/)

[Cleaning Data with PySpark](https://www.datacamp.com/courses/cleaning-data-with-pyspark)

[Source of countries_of_the_world.csv](https://www.kaggle.com/fernandol/countries-of-the-world)