from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import rank

spark = SparkSession.builder.appName("mysql-etl").getOrCreate()

imdb_data = spark.read.parquet("gs://partitioned-imdb/imdb_start_year/")
imdb_data.createOrReplaceTempView("startYear")

sql = """
    select
        count(*) as total_movies
    from 
        startYear 
    """

df_result = spark.sql(sql)
df_result.show()

df_result.write.mode('overwrite').json("gs://partitioned-imdb/count_result/")

ratings = spark.read.csv("gs://pyspark-fs-xsan/title.ratings.tsv", header=True, inferSchema=True, sep="\t")
movie_ratings = imdb_data.join(ratings, imdb_data.tconst == ratings.tconst).drop(ratings.tconst)
movie_ratings.createOrReplaceTempView("movie_ratings")
movies_by_genre_by_year = movie_ratings.withColumn("genre", explode(F.split(movie_ratings["genres"], ","))) \
    .groupBy("startYear", "genre") \
    .agg(F.count("tconst").alias("movieCount"))
movies_by_genre_by_year.createOrReplaceTempView("movies_by_genre_by_year")
windowSpec = Window.partitionBy(movies_by_genre_by_year["startYear"]).orderBy(
    movies_by_genre_by_year["movieCount"].desc())
movies_by_genre_by_year = movies_by_genre_by_year.withColumn("rank", rank().over(windowSpec))
top_genres_by_year = movies_by_genre_by_year.filter(movies_by_genre_by_year["rank"] <= 5).orderBy(
    movies_by_genre_by_year["startYear"])
top_genres_by_year.tail(20)
top_genres_by_year.write.mode('overwrite').json("gs://partitioned-imdb/rank_result/")
