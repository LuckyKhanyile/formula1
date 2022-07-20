# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingest qualifying.json file

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

qualifying_schema = StructType(fields = [
  StructField("qualifyId" ,IntegerType(), False),
    StructField("raceId" ,IntegerType(), False),
    StructField("driverId" ,IntegerType(), True),
    StructField("constructorId" ,IntegerType(), True),
    StructField("number" ,IntegerType(), True),
    StructField("position" ,IntegerType(), True),
    StructField("q1" ,StringType(),True),
    StructField("q2" ,StringType(), True),
    StructField("q3" ,StringType(), True)
])

# COMMAND ----------

qualifying_df = spark.read.schema(qualifying_schema).option("multiLine",True).json(f"{raw_folder_path}/{v_file_date}/qualifying")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Rename columns and add new columns

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

qualifying_final_df = add_ingestion_date(qualifying_df\
.withColumnRenamed("qualifyId","qualify_id")\
.withColumnRenamed("raceId","race_id")\
.withColumnRenamed("driverId","driver_id")\
.withColumnRenamed("constructorId","constructor_id"))\
.withColumn("data_source",lit(v_data_source))\
.withColumn("file_date", lit(v_file_date))
#.withColumn("ingestion_date",current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Write output to processed container in parquet format

# COMMAND ----------

#qualifying_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/qualifying")
#qualifying_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.qualifying")
overwrite_partition(qualifying_final_df,'f1_processed','qualifying','race_id')

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from f1_processed.qualifying;

# COMMAND ----------

#display(spark.read.parquet("/mnt/4mula1dl/processed/qualifying"))

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

