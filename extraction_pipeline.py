from pyspark.sql import SparkSession
import mysql.connector
import pandas as pd

# Initialize a Spark session
spark = SparkSession.builder.appName("pyspark-mysql-extraction").getOrCreate()

# Define MySQL connection parameters
user = 'root'
password = 'admin'
host = '34.31.202.163'
db_name = 'imdb'

# Establish a connection to the MySQL server
connection = mysql.connector.connect(
    user=user,
    password=password,
    database=db_name,
    host=host,
    port='3306'
)

# Read the data from the 'imdb_data' table in the 'imdb' database into a pandas DataFrame
pd_data = pd.read_sql("SELECT * FROM imdb_data", con=connection)

# Close the MySQL server connection
connection.close()

# Convert the pandas DataFrame to a Spark DataFrame
df_data = spark.createDataFrame(pd_data)

# Write the data to Google Cloud Storage (GCS), partitioning by 'startYear', in Parquet format
df_data.write.partitionBy('startYear').mode('overwrite').parquet("gs://partitioned-imdb/imdb_start_year/")