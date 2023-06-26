# sqoop command

sqoop import \
--connect jdbc:mysql://<HOST>:3306/movieDB \
--username <USERNAME> \
--password <PASSWORD> \
--table <TABLE> \
--target-dir /user/movies \


# hdfs commands

hadoop fs -mkdir /user/movies

hadoop fs -put movie.csv /user/movies/

hadoop fs -put rating.csv /user/movies/


# hive table

create database moviedb

use moviedb

create table if not exists rating(
user_id bigint,
movie_id bigint,
ratings float,
viewdt bigint
)
row format delimited
fields terminated by ",";


create table if not exists movie(
movie_id bigint,
moviename String,
genre String
)
row format delimited
fields terminated by ",";

create table if not exists TopMovies(
moviename String,
genre String,
ratings float
)
row format delimited
fields terminated by ","
TBLPROPERTIES("skip.header.line.count"="1");


# load data into tables

load data inpath "/user/movies/movie.csv"
into table movie;

load data inpath "/user/movies/rating.csv"
into table rating;








