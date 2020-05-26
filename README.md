# data-eng-capstone
Data Engineering Capstone Project

### Step 1: Scope the Project and Gather Data

#### Scope 
Explain what you plan to do in the project in more detail. What data do you use? What is your end solution look like? What tools did you use? etc>

#### Describe and Gather Data 
Describe the data sets you're using. Where did it come from? What type of information is included?

### Step 2: Explore and Assess the Data

#### Explore the Data 
Identify data quality issues, like missing values, duplicate data, etc.

#### Cleaning Steps
Document steps necessary to clean the data

### Step 3: Define the Data Model

#### 3.1 Conceptual Data Model
Map out the conceptual data model and explain why you chose that model

#### 3.2 Mapping Out Data Pipelines
List the steps necessary to pipeline the data into the chosen data model

### Step 4: Run Pipelines to Model the Data

#### 4.1 Create the data model
Build the data pipelines to create the data model.

#### 4.2 Data Quality Checks
Explain the data quality checks you'll perform to ensure the pipeline ran as expected. These could include:
 * Integrity constraints on the relational database (e.g., unique key, data type, etc.)
 * Unit tests for the scripts to ensure they are doing the right thing
 * Source/Count checks to ensure completeness

I compared arrivals data from the government's official [website](https://travel.trade.gov/view/m-2017-I-001/index.asp) to a report I generated using the following command:

```
# I-94 Arrivals by Country of Residence (COR) and Month
# Exludes Mexico to focus on Overseas countries
df.filter(~ df.i94res_desc.contains('MEXICO')) \
    .groupBy('i94res_desc', 'i94mon') \
    .count() \
    .toPandas() \
    .to_csv('out.csv', index=False)
```

#### 4.3 Data dictionary 
Create a data dictionary for your data model. For each field, provide a brief description of what the data is and where it came from. You can include the data dictionary in the notebook or in a separate file.

#### Step 5: Complete Project Write Up
* Clearly state the rationale for the choice of tools and technologies for the project.
* Propose how often the data should be updated and why.
* Write a description of how you would approach the problem differently under the following scenarios:
 * The data was increased by 100x.
 * The data populates a dashboard that must be updated on a daily basis by 7am every day.
 * The database needed to be accessed by 100+ people.

### Setup

I ran development versions of my PySpark code in Local Mode on my Mac which has the following hardware configuration:

* MacBook Pro (Retina, 13-inch, Early 2013)
* OS: macOS Catalina, Version 10.15.4
* Processor: 3 GHz Dual-Core Intel Core i7
* Memory: 8 GB 1600 MHz DDR3
* HD: 500 GB Flash Storage (432.1 GB Available)

#### Downloading the I-94 data

From a terminal in Udacity's JupyterLab environment, I compressed the I-94 data by typing the following from the default prompt (/home/workspace):

```
tar -zcvf data.tar.gz ../../data
```
z - use gzip  
c - create new archive  
v - verbose  
f - use given archive file  

After downloading `data.tar.gz`, I then uncompressed the files using my Mac's built-in Archive Utility. The result was 12 files:

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