# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingest constructors.json file

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
# MAGIC ##### Step 1 - Read JSON fle using the spark dataframe reader

# COMMAND ----------

constructors_schema = "constructorId INT, constructorRef String, name String, nationality String, url String"

# COMMAND ----------

constructor_df = spark.read\
.schema(constructors_schema)\
.json(f"{raw_folder_path}/{v_file_date}/constructors.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Drop unwanted columns from dataframe

# COMMAND ----------

constructor_dropped_df=constructor_df.drop(constructor_df['url'])

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Rename columns and add ingestion date

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

constructor_final_df = constructor_dropped_df.withColumnRenamed("constructorId","constructor_id")\
                                              .withColumnRenamed("constructorRef","constructor_ref")\
                                              .withColumn("ingestion_date",current_timestamp())\
                                              .withColumn("data_source",lit(v_data_source))\
                                              .withColumn("file_date",lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 4 - Write output to parquet

# COMMAND ----------

#constructor_final_df.write.parquet("/mnt/4mula1dl/processed/constructors",mode="overwrite")
constructor_final_df.write.format("parquet").mode("overwrite").saveAsTable("f1_processed.constructors")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.constructors

# COMMAND ----------

#display(spark.read.parquet("/mnt/4mula1dl/processed/constructors"))

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

