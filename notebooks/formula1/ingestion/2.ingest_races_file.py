# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingest race.csv File

# COMMAND ----------

# MAGIC %run "../includes/configurations"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_data_source","","Data Source")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date","","File Date")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1. Read the CSV file using the spark reader API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType

# COMMAND ----------

races_schema = StructType(fields=[
    StructField("raceId", IntegerType(), False),
    StructField("year", IntegerType(), True),
    StructField("round", IntegerType(), True),
    StructField("circuitId", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("date", StringType(), True),
    StructField("time", StringType(), True),
    StructField("url", StringType(), True)
])

# COMMAND ----------

races_df = spark.read.option("header",True).schema(races_schema).csv(f"{raw_folder_path}/{v_file_date}/races.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2. Add ingestion date and race_timestamp to the dataframe

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, to_timestamp, concat, col, lit

# COMMAND ----------

races_with_timestamp_df = races_df.withColumn("ingestion_date",current_timestamp())\
.withColumn("race_timestamp",to_timestamp(concat(col('date'),lit(' '),col('time')),"yyyy-MM-dd HH:mm:ss"))

# COMMAND ----------

# MAGIC %md
# MAGIC #####3. Select only the required columns and rename as required

# COMMAND ----------

races_selected_df = races_with_timestamp_df.select(
    col('raceId').alias('race_id'),
    col('year').alias('race_year'),
    col('round'),
    col('circuitId').alias('circuit_id'),
    col('name'),
    col('race_timestamp'),
    col('ingestion_date')
).withColumn("data_source",lit(v_data_source)).withColumn("file_date",lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 4.Write the output to processed container in paruet format

# COMMAND ----------

#races_selected_df.write.partitionBy('race_year').parquet("/mnt/4mula1dl/processed/races",mode="overwrite")
races_selected_df.write.partitionBy('race_year').format("parquet").mode("overwrite").saveAsTable("f1_processed.races")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/4mula1dl/processed

# COMMAND ----------

# MAGIC %sql
# MAGIC select race_id,count(1) as records from f1_processed.races
# MAGIC group by race_id

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

