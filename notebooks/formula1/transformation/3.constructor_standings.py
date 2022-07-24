# Databricks notebook source
# MAGIC %run "../includes/configurations"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-21","File Date")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

race_results_list = spark.read.parquet(f"{presentation_folder_path}/race_results").filter(f"file_date='{v_file_date}'")

# COMMAND ----------

#race_year_list = []
#for race_year in race_results_list:
#    race_year_list.append(race_year.race_year)
#print(race_year_list)
race_year_list=df_column_to_list(race_results_list,'race_year')
print(race_year_list)

# COMMAND ----------

from pyspark.sql.functions import sum, when, count, col

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results").filter(col("race_year").isin(race_year_list))

# COMMAND ----------

constructor_standings_df = race_results_df\
.groupBy("race_year","team")\
.agg(
    sum("points").alias("total_points"),
     count(when(col("position")==1,True)).alias("wins")
    )

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank
driver_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"),desc("wins"))
final_df = constructor_standings_df.withColumn("rank", rank().over(driver_rank_spec))

# COMMAND ----------

#final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.constructor_standings")
overwrite_partition(final_df,'f1_presentation','constructor_standings','race_year')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_presentation.constructor_standings;

# COMMAND ----------

dbutils.notebook.exit("SUCCESS")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE f1_presentation.constructor_standings;

# COMMAND ----------

