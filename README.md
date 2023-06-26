# Top_Movies

The code for hadoop,hive and pyspark can be found
 
 # About
 In this project I have worked on two csv files
 1. movie.csv
 2. rating.csv

First Ingested the datasets from rdbms to hdfs using sqoop

Created hive table on the top of the datasets .Additionally created an empty table for highest rated movies.

• Created most watched highest rated movies list .Considering movies only with views >2000 
Populated the empty table in hive with spark SQL queries

• Created partitions on the basis of Alphabets of movies using pyspark transformations on hive tables to create
different folder for each alphabet

• Created partitions on the basis of Genre of movies using pyspark to create different folder for every genre in
hdfs 

My project is on GCP dataproc, however it can be done on Linux also.

# Dependencies
1. Hive
2. Hadoop
3. Pyspark(3.3.2)
4. GCP(dataproc)
