# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingest lap_times.csv file

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
# MAGIC ##### Step 1 - Read the CSV file using the spark dataframe reader API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# COMMAND ----------

lap_time_schema = StructType(fields = [
    StructField("raceId" ,IntegerType(), False),
    StructField("driverId" ,IntegerType(), True),
    StructField("lap" ,IntegerType(), True),
    StructField("position" ,IntegerType(), True),
    StructField("time" ,StringType(),True),
    StructField("milliseconds" ,IntegerType(), True)
])

# COMMAND ----------

lap_times_df = spark.read.schema(lap_time_schema).csv(f"{raw_folder_path}/{v_file_date}/lap_times")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Rename columns and add new columns

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

lap_times_final_df = lap_times_df\
.withColumnRenamed("raceId","race_id")\
.withColumnRenamed("driverId","driver_id")\
.withColumn("ingestion_date",current_timestamp())\
.withColumn("data_source",lit(v_data_source)) \
.withColumn("file_date",lit(v_file_date)) 


# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Write output to processed container in parquet format

# COMMAND ----------

#lap_times_final_df.write.mode("overwrite").parquet("/mnt/4mula1dl/processed/lap_times")
#lap_times_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.lap_times")
overwrite_partition(lap_times_final_df,'f1_processed','lap_times','race_id')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.lap_times;

# COMMAND ----------

display(spark.read.parquet("/mnt/4mula1dl/processed/lap_times"))

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

