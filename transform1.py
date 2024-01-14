from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, count, rank
from pyspark.sql.window import Window

# Initialize spark session
spark = SparkSession.builder.appName("mysql-etl").getOrCreate()

# Read from the partitioned data
imdb_data = spark.read.parquet("gs://partitioned-imdb/imdb_start_year/")
imdb_data.createOrReplaceTempView("startYear")

# Count total movies
sql = """
    select
        count(*) as total_movies
    from 
        startYear 
    """

total_movies = spark.sql(sql)
total_movies.show()

# Save results
total_movies.write.mode('overwrite').json("gs://partitioned-imdb/count_result/")

# the total count of movies in the provided dataset and an annual
# ranking of movie genres based on the number of movies in each
# genre, with both sets of data outputted as JSON files.

# Load ratings data
ratings = spark.read.csv("gs://pyspark-fs-xsan/title.ratings.tsv", header=True, inferSchema=True, sep="\t")

# Join the movie and ratings datasets
movie_ratings = imdb_data.join(ratings, imdb_data.tconst == ratings.tconst).drop(ratings.tconst)
movie_ratings.createOrReplaceTempView("movie_ratings")

# Split genres, group by startYear and genre, count movies and rank them
movies_by_genre_by_year = movie_ratings.withColumn("genre", explode(split(movie_ratings["genres"], ","))) \
    .groupBy("startYear", "genre") \
    .agg(count("tconst").alias("movieCount"))

movies_by_genre_by_year.createOrReplaceTempView("movies_by_genre_by_year")

windowSpec = Window.partitionBy(movies_by_genre_by_year["startYear"]).orderBy(
    movies_by_genre_by_year["movieCount"].desc())
movies_by_genre_by_year = movies_by_genre_by_year.withColumn("rank", rank().over(windowSpec))

# Filter top 5 genres by year
top_genres_by_year = movies_by_genre_by_year.filter(movies_by_genre_by_year["rank"] <= 5).orderBy(
    movies_by_genre_by_year["startYear"])

top_genres_by_year.tail(20)

# Save results
top_genres_by_year.write.mode('overwrite').json("gs://partitioned-imdb/rank_result/")
