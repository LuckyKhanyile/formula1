# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingest pit_stops.json file

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
# MAGIC ##### Step 1 - Read the JSON file using the spark dataframe reader API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# COMMAND ----------

pit_stops_schema = StructType(fields = [
    StructField("raceId" ,IntegerType(), False),
    StructField("driverId" ,IntegerType(), True),
    StructField("stop" ,IntegerType(), True),
    StructField("lap" ,IntegerType(), True),
    StructField("time" ,StringType(),True),
    StructField("duration" ,StringType(),True),
    StructField("milliseconds" ,IntegerType(), True)
])

# COMMAND ----------

pit_stops_df = spark.read.schema(pit_stops_schema).option("multiLine",True).json(f"{raw_folder_path}/{v_file_date}/pit_stops.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Rename columns and add new columns

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

pit_stops_final_df = pit_stops_df\
.withColumnRenamed("raceId","race_id")\
.withColumnRenamed("driverId","driver_id")\
.withColumn("ingestion_date",current_timestamp())\
.withColumn("data_source",lit(v_data_source))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Write output to processed container in parquet format

# COMMAND ----------

#pit_stops_final_df.write.mode("overwrite").parquet("/mnt/4mula1dl/processed/pit_stops")
#pit_stops_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.pit_stops")
overwrite_partition(pit_stops_final_df,'f1_processed','pit_stops','race_id')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.pit_stops;

# COMMAND ----------

#display(spark.read.parquet("/mnt/4mula1dl/processed/pit_stops"))

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

