# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE
# MAGIC   ncaab_ratings (
# MAGIC     rank INT,
# MAGIC     team STRING,
# MAGIC     conference STRING,
# MAGIC     AdjEM DOUBLE,
# MAGIC     AdjOE DOUBLE,
# MAGIC     AdjOE_Rk INT,
# MAGIC     AdjDE DOUBLE,
# MAGIC     AdjDE_Rk INT,
# MAGIC     AdjT DOUBLE,
# MAGIC     AdjT_Rk INT,
# MAGIC     Luck DOUBLE,
# MAGIC     Luck_Rk INT,
# MAGIC     SOS_Rating DOUBLE,
# MAGIC     SOS_Rating_Rk INT,
# MAGIC     Opp_AdjO DOUBLE,
# MAGIC     Opp_AdjO_Rk INT,
# MAGIC     Opp_AdjD DOUBLE,
# MAGIC     Opp_AdjD_Rk INT,
# MAGIC     NCSOS_Rating DOUBLE,
# MAGIC     NCSOS_Rating_Rk INT,
# MAGIC     MM_Seed INT,
# MAGIC     Wins INT,
# MAGIC     Losses INT
# MAGIC   );
# MAGIC
# MAGIC INSERT INTO
# MAGIC   ncaab_ratings
# MAGIC SELECT
# MAGIC     Rank,
# MAGIC     Team,
# MAGIC     Conference,
# MAGIC     AdjEM,
# MAGIC     AdjOE,
# MAGIC     AdjOE_Rk,
# MAGIC     AdjDE,
# MAGIC     AdjDE_Rk,
# MAGIC     AdjT,
# MAGIC     AdjT_Rk,
# MAGIC     Luck,
# MAGIC     Luck_Rk,
# MAGIC     SOS_Rating,
# MAGIC     SOS_Rating_Rk,
# MAGIC     Opp_AdjO,
# MAGIC     Opp_AdjO_Rk,
# MAGIC     Opp_AdjD,
# MAGIC     Opp_AdjD_Rk,
# MAGIC     NCSOS_Rating,
# MAGIC     NCSOS_Rating_Rk,
# MAGIC     MM_Seed,
# MAGIC     Wins,
# MAGIC     Losses
# MAGIC FROM
# MAGIC   global_temp.ratings_df_shared
