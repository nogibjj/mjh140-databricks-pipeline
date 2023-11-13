# Databricks notebook source
# Install kenpom web scraper
%pip install kenpompy > /dev/null

# COMMAND ----------

import datetime
import pyodbc
import pandas as pd
from kenpompy.utils import login
from kenpompy.misc import get_pomeroy_ratings
from kenpompy.summary import get_fourfactors, get_pointdist
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructType, StructField


# COMMAND ----------

# Returns an authenticated browser that can then be used to scrape pages that require authorization.
browser = login('matthew.holden@duke.edu', 'ncaabstats24')

# COMMAND ----------

# Scrapes the Pomeroy College Basketball Ratings table (https://kenpom.com/index.php) into a dataframe.
ratings = get_pomeroy_ratings(browser, season='2024')

# COMMAND ----------

# Clean Ratings Table 

# Split Wins and Losses
ratings[['Wins', 'Losses']] = ratings['W-L'].str.split('-', expand=True)
ratings = ratings.drop('W-L', axis=1)

# Remove header rows from scraped data
ratings = ratings.dropna(subset=['Wins'])

# Datatype conversion indexes
dub_idx = [3, 4, 6, 8, 10, 12, 14, 16, 18, 20]
int_idx = [0, 5, 7, 9, 11, 13, 15, 17, 19, 21, 22]
str_idx = [1, 2]

# Datatype conversions
for idx in dub_idx:
    ratings.iloc[:, idx] = pd.to_numeric(ratings.iloc[:, idx],  errors = 'coerce')
for idx in int_idx:
    ratings.iloc[:, idx] = pd.to_numeric(ratings.iloc[:, idx],  errors = 'coerce')
for idx in str_idx:
    ratings.iloc[:, idx] = ratings.iloc[:, idx].astype(dtype = 'object')

# Remove header rows from scraped data
ratings = ratings.dropna(subset=['Wins'])


# COMMAND ----------

# Define the schema for the DataFrame (must match the Pandas DataFrame schema)
schema = StructType(
  [
    StructField("Rank", IntegerType(), True),
    StructField("Team", StringType(), False),
    StructField("Conference", StringType(), True),
    StructField("AdjEM", DoubleType(), True),
    StructField("AdjOE", DoubleType(), True),
    StructField("AdjOE_Rk", IntegerType(), True),
    StructField("AdjDE", DoubleType(), True),
    StructField("AdjDE_Rk", IntegerType(), True),
    StructField("AdjT", DoubleType(), True),
    StructField("AdjT_Rk", IntegerType(), True),
    StructField("Luck", DoubleType(), True),
    StructField("Luck_Rk", IntegerType(), True),
    StructField("SOS_Rating", DoubleType(), True),
    StructField("SOS_Rating_Rk", IntegerType(), True),
    StructField("Opp_AdjO", DoubleType(), True),
    StructField("Opp_AdjO_Rk", IntegerType(), True),
    StructField("Opp_AdjD", DoubleType(), True),
    StructField("Opp_AdjD_Rk", IntegerType(), True),
    StructField("NCSOS_Rating", DoubleType(), True),
    StructField("NCSOS_Rating_Rk", IntegerType(), True),
    StructField("MM_Seed", IntegerType(), True),
    StructField("Wins", IntegerType(), True),
    StructField("Losses", IntegerType(), True)
  ]
)

# Convert the Pandas DataFrame to a Spark DataFrame
ratings_df = spark.createDataFrame(ratings, schema=schema)

ratings_df.createGlobalTempView("ratings_df_shared")
