# Udacity Data Engineering Nano Degree Project; Data Warehouse

##Schema for Song Play Analysis

Using the song and event datasets, you'll need to create a star schema optimized for queries on song play analysis. This includes the following tables.

### Fact Table

1. songplays - records in event data associated with song plays i.e. records with page NextSong
    * songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

### Dimension Tables
2. users - users in the app
    * user_id, first_name, last_name, gender, level
3. songs - songs in music database
    * song_id, title, artist_id, year, duration
4. artists - artists in music database
    * artist_id, name, location, lattitude, longitude
5. time - timestamps of records in songplays broken down into specific units
    * start_time, hour, day, week, month, year, weekday
6. Project Template
    * To get started with the project, go to the workspace on the next page, where you'll find the project template. You can work on your project and submit your work through this workspace.

The project template includes three files:

`create_table.py` is where you'll create your fact and dimension tables for the star schema in Redshift.
`etl.py` is where you'll load data from S3 into staging tables on Redshift and then process that data into your analytics tables on Redshift.
`sql_queries.py` is where you'll define you SQL statements, which will be imported into the two other files above.

## Discussion Questions:

### Database Purpose:

A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

The purpose of this database is to collect and manage user data and interactions. With the managed data source, Sparkify aims to use analytics to understand the product and users more deeply in order to enhance both the product and grow the company.

### Schema Design:

The schema for this database is configured intwo two distinct parts:

1. Staging
  * This is the initial ingestion point of the raw data from the specified s3 buckets. This data has not been split into dimension and facts but will serve as the initial foundation on which to design a star schema
2. Dimension and Fact tables:
  * From the raw data ingested from the S3 buckets, the `etl.py` will break the data out into dimension and fact tables. Breaking out these tables allows for better data management and computational efficiency. 

