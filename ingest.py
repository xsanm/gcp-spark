# # import pandas as pd
# # from sqlalchemy import create_engine
# #
# # # Read and process data from the TSV file
# # df = pd.read_csv('title.basics.tsv', sep='\t')
# #
# # # MySQl Connection details
# # host = '34.31.202.163'
# # user = 'root'
# # password = 'admin'
# # db = 'imdb'
# #
# # # Create a SQLAlchemy engine
# # engine = create_engine(f'mysql+pymysql://{user}:{password}@{host}/{db}')
# #
# #
# # def process_data(df, chunksize, engine):
# #     # Calculate number of chunks
# #     n = 10
# #     print(f'Inserting {n} rows...')
# #
# #     # Insert data in chunks
# #     for i in range(0, n, chunksize):
# #         chunk = df[i:i + chunksize]
# #         chunk.to_sql(name='imdb_data', con=engine, if_exists='append', index=False)
# #         print(f"Inserted rows up to index: {min(i + chunksize, n)}")
# #
# #     print('Finished.')
# #
# #
# # # Process DataFrame
# # process_data(df, chunksize=1000, engine=engine)
#
#
#
# import pymysql
#
# # Connect to the database
# connection = pymysql.connect(
#     host='34.31.202.163',
#     user='root',
#     password='admin',
#     database='imdb')
#
# cursor = connection.cursor()
#
# # Execute a query to get the top 10 rows
# cursor.execute("SELECT * FROM imdb_data LIMIT 10")
#
# # Fetch results
# results = cursor.fetchall()
# for row in results:
#     print(row)
#
# # Close the connection
# connection.close()


import pandas as pd
from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.sql import select

# Database credentials
user = 'root'
password = 'admin'
host = '34.31.202.163'
db_name = 'imdb'

# Create connection string and engine
engine = create_engine("mysql+pymysql://{user}:{password}@{host}/{db_name}")

# Load the TSV file to a DataFrame
df = pd.read_csv('title.basics.tsv', sep='\t', na_values='\\N')
df['isAdult'] = df['isAdult'].astype(bool)
df.loc[df['runtimeMinutes'].astype(str).str.isdigit() == False, 'runtimeMinutes'] = -1

# Replace the NaN values to appropriate substitute for SQL
df = df.fillna({
    'startYear': -1,
    'endYear': -1,
    'runtimeMinutes': -1,
    'genres': ''
})

# Convert the column types
df = df.astype({
    'startYear': 'int32',
    'endYear': 'int32',
    'runtimeMinutes': 'int32'
})

# Define chunk size
chunksize = 1000

# Upload the DataFrame to MySQL in chunks
for i in range(0, len(df), chunksize):
    print(f"Inserting rows {i} to {i + chunksize}")
    df[i:i + chunksize].to_sql(
        name='imdb_data',
        con=engine,
        if_exists='append',
        index=False,
        method='multi'
    )
print("Data upload to MySQL complete!")