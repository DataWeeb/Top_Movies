from pyspark.sql import SparkSession

# Create a SparkSession with Hive support
spark = SparkSession.builder.appName("sparkHiveBase").enableHiveSupport().getOrCreate()


# Set logging level to error
spark.sparkContext.setLogLevel("ERROR")

db="moviedb"

movie = "movie"

ratings = "rating"

TopMovies="topmovies"

# Hive query to retrieve the data
df = spark.sql(f""" with cte as 
              (SELECT user_id,movie_id,avg(ratings) as ratings FROM {db}.{ratings}
              GROUP BY user_id, movie_id),
              
              cte2 as 
              (select movie_id,count(*) as views,avg(ratings) as ratings from cte 
              group by movie_id),
              
              cte3 as
              (select movie_id,views,ratings from cte2 where views >2000)
              
            
              select m.moviename,m.genre,c.ratings
              from {db}.{movie} m 
              join cte3 c on c.movie_id=m.movie_id 
              order by ratings desc
              
              

""")


df.write.mode("overwrite").saveAsTable(f"{db}.{TopMovies}")



















