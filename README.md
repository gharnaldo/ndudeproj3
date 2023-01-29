# Project: Data Warehouse

## Project Description

This Project will allow Sparkify Startup analyze the data they've been collecting on songs and user activity on their new music streaming app. They will be able to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

The Project carried out several tasks including a Redshift cluster creation on AWS and all the processes than moves data from S3 to the staging tables on Redshift and an ETL that feed an star schema from the staging tables where users will be able to perform queries.


***

## Scripts execution with python from the project's root

### Create Tables on Redshift AWS

    py create_tables.py

### Execute ETL

    py etl.py

***

## Files in repository

1. **create_tables.py** is where you'll create your fact and dimension tables for the star schema in Redshift.
2. **etl.py** is where you'll load data from S3 into staging tables on Redshift and then process that data into your analytics tables on Redshift.
3. **sql_queries.py** is where you'll define you SQL statements, which will be imported into the two other files above.


