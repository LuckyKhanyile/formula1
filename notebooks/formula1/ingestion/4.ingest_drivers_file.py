# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingest drivers.json file

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
# MAGIC ##### Step 1 - Read the JSON file using spark dataframe reader API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

# COMMAND ----------

name_schema = StructType(fields=[
    StructField("forename", StringType(), True),
    StructField("surname", StringType(), True)
])

# COMMAND ----------

drivers_schema = StructType(fields=[
    StructField("driverId", IntegerType(), False),
    StructField("driverRef", StringType(), True),
    StructField("number", IntegerType(), True),
    StructField("code", StringType(), True),
    StructField("name", name_schema),
    StructField("dob", DateType(), True),
    StructField("nationality", StringType(), True),
    StructField("url", StringType(), True)
])

# COMMAND ----------

drivers_df = spark.read\
.schema(drivers_schema)\
.json(f"{raw_folder_path}/{v_file_date}/drivers.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Rename columns and add new columns

# COMMAND ----------

from pyspark.sql.functions import col, concat, current_timestamp, lit

# COMMAND ----------

drivers_with_columns_df = drivers_df.withColumnRenamed("driverId", "driver_id")\
                                    .withColumnRenamed("driverRef", "driver_ref")\
                                    .withColumn("ingestion_date", current_timestamp())\
                                    .withColumn("name", concat(col("name.forename"),lit(" "),col("name.surname")))\
                                    .withColumn("data_source",lit(v_data_source))\
                                    .withColumn("file_date",lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Drop the unwanted columns

# COMMAND ----------

drivers_final_df = drivers_with_columns_df.drop(col("url"))

# COMMAND ----------

#drivers_final_df.write.parquet("/mnt/4mula1dl/processed/drivers",mode="overwrite")
drivers_final_df.write.format("parquet").mode("overwrite").saveAsTable("f1_processed.drivers")

# COMMAND ----------

#display(spark.read.parquet("/mnt/4mula1dl/processed/drivers"))

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

