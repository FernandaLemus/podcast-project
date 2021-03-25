# podcast-project
Capstone project of Data Engineer Nanodegree from Udacity

### Data Engineering Capstone Project

#### Project Summary

iTunes has podcasts listed on its platform, and as an additional feauture you can rate their content and leave a review. The Analytics team would like to have the top podcast with the best average rating, as well as a top by category.

These ratings and reviews live in different tables, and the content of the reviews exceeds one million rows, so the data engineering team has been asked to create two tables with the average of ratings per podcast and per category.

The project follows the follow steps:
* Step 1: Scope the Project and Gather Data

#### Scope 

We will work with three data sets:
    
    1. categories: categorie of each podcast
    2. podcast: general data of each podcast (name, url, id, etc.)
    3. reviews: all user reviews on podcasts
   
And the main goal is to bring the data from s3 into readshift and run a ETL that create the two new tables _podcast__stats_ and _categories_stats_. 

We're gonna use the copy command to bring the data to redshift and run some queries in redshift itself for filling tables.


* Step 2: Explore and Assess the Data

#### Explore the Data 

Identify data quality issues, like missing values, duplicate data, etc.

#### Cleaning Steps
There's no cleaning steps for this data sets. 

* Step 3: Define the Data Model

More detail on de data model on DDL folder

* Step 4: Run ETL to Model the Data

The etl model is defined through an Airflow DAG. I ran this project locally in the Data Pipelines with Airflow workspace and all the necessary files are in the Airflow folder.

