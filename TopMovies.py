from pyspark.sql import SparkSession
import string

# Create a SparkSession with Hive support
spark = SparkSession.builder.appName("sparkHiveBase").enableHiveSupport().getOrCreate()


# Set logging level to error
spark.sparkContext.setLogLevel("ERROR")

db="moviedb"

movie = "movie"

ratings = "rating"

TopMovies="topmovies"

# Hive query to retrieve the data of most watched high rated movies
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


####partition by alphabets

first_letter=[letter for letter in string.ascii_uppercase]

for i in first_letter:
    df=spark.sql(f"""select * from movie where substring(moviename,1,1)='{i}' order by movie_id""")
    partition_path = "/user/movie"

    df.write.mode("overwrite").csv(partition_path)

#it will create folders for every alphabet from A-Z

####partition by genre


from pyspark.sql.functions import col,split,explode


df=movie.withColumn("genre_list",split(col("genre"),"\\|")).drop("genre")

df2=df.withColumn("genre",explode(df["genre_list"]))


df3= df2.select("movie_id", "moviename", "genre")

partition_path = "/user/movie"

df3.write.mode("overwrite").csv(partition_path)

#note# it will create folders according to genres
























