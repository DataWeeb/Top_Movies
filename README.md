# Top_Movies

The code for hadoop,hive and pyspark can be found
 
 # About
 In this project I have worked on two csv files
 1. movie.csv
 2. rating.csv

my target in this project was to get the most watched highest rated movies from these two files

I have used hive for creating tables and does transformations on that using spark SQL queries 
and loaded the data into a already created empty hive table

so as a result we have a separate table for the result I wanted

Other way I know to do it is, you can simply load data from  pyspark to hdfs and then create table on top of it.

My project is on GCP dataproc, however it can be done on Linux also.

# Dependencies
1. Hive
2. Hadoop
3. Pyspark(3.3.2)
4. GCP(dataproc)
