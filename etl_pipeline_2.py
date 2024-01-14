from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import rank

spark = SparkSession.builder.appName("mysql-etl-2").getOrCreate()

imdb_data = spark.read.parquet("gs://partitioned-imdb/imdb_start_year/")
imdb_data.createOrReplaceTempView("startYear")


ratings = spark.read.csv("gs://pyspark-fs-xsan/title.ratings.tsv", header=True, inferSchema=True, sep="\t")

ratings.createOrReplaceTempView("ratings")

# Perform join and grouping query
best = spark.sql("""
    SELECT m.startYear, m.primaryTitle, r.averageRating, r.numVotes
    FROM startYear m
    JOIN ratings r ON r.tconst = m.tconst
    WHERE m.titleType = 'movie'
    GROUP BY m.startYear, m.primaryTitle, r.averageRating, r.numVotes
    ORDER BY m.startYear, r.averageRating DESC, r.numVotes DESC
""")

best.tail(20)

best.write.mode('overwrite').json("gs://partitioned-imdb/best_result/")