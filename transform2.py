from pyspark.sql import SparkSession
from pyspark.sql.functions import rank
from pyspark.sql.window import Window

# Initialize Spark Session
spark = SparkSession.builder.appName("mysql-etl-2").getOrCreate()

# Read movie dataset
imdb_data = spark.read.parquet("gs://partitioned-imdb/imdb_start_year/")
imdb_data.createOrReplaceTempView("startYear")

# Read ratings dataset
ratings = spark.read.csv("gs://pyspark-fs-xsan/title.ratings.tsv", header=True, inferSchema=True, sep="\t")

movie_ratings = imdb_data.join(ratings, imdb_data.tconst == ratings.tconst).drop(ratings.tconst)
movie_ratings.createOrReplaceTempView("ratings")

# The script retrieves the title of each movie,
# its release year, average rating, and the number of
# votes it received, sorts these movies by their release year,
# rating, and vote count in descending order, and saves this data as a JSON file.

# Perform join between movie and ratings dataset and
# group by start year, primary title, average ratings, and number of votes.
# It returns movies sorted by year of release, average rating and number of votes.
# Perform join and grouping query
windowSpec = Window.partitionBy("startYear").orderBy(movie_ratings["averageRating"].desc(), movie_ratings["numVotes"].desc())

# Add Rank to each row within the Window
best = movie_ratings.withColumn("rank", rank().over(windowSpec))

# Filter to keep only the 'best' movie for each year
best_movie_per_year = best.filter(best["rank"] == 1)

# Write the result as a JSON file to a specific location
best.write.mode('overwrite').json("gs://partitioned-imdb/best_result/")