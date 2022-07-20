# Databricks notebook source
# MAGIC %md
# MAGIC ###Ingest circuit.csv

# COMMAND ----------

# MAGIC %run "../includes/configurations"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_data_source","","Data Source")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-21","File Date")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Read the csv file using the spark dataframe reader  

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# COMMAND ----------

circuits_schema = StructType(fields=[
    StructField("circuitId", IntegerType(), False),
    StructField("circuitRef", StringType(), True),
    StructField("name", StringType(), True),
    StructField("location",StringType(), True),
    StructField("country",StringType(), True),
    StructField("lat",DoubleType(), True),
    StructField("lng",DoubleType(),True),
    StructField("alt",IntegerType(), True),
    StructField("url",StringType(),True)
])

# COMMAND ----------

circuit_df = spark.read\
.option("header",True)\
.schema(circuits_schema)\
.csv(f"{raw_folder_path}/{v_file_date}/circuits.csv")


# COMMAND ----------

# MAGIC %md
# MAGIC #####Step 2 - Select only the required columns

# COMMAND ----------

#circuits_selected_df = circuit_df.select("circuitId","circuitRef","name","location","country","lat","lng","alt")

# COMMAND ----------

#circuits_selected_df = circuit_df.select(circuit_df["circuitId"],circuit_df["circuitRef"],circuit_df["name"],
#                                         circuit_df["location"],circuit_df["country"],circuit_df["lat"],circuit_df["lng"],circuit_df["alt"].alias("latitude"))

# COMMAND ----------

#circuits_selected_df = circuit_df.select(circuit_df.circuitId,circuit_df.circuitRef,circuit_df.name,
#                                         circuit_df.location,circuit_df.country.alias("race_country"),circuit_df.lat,circuit_df.lng,circuit_df.alt)

# COMMAND ----------

from pyspark.sql.functions import col 

# COMMAND ----------

circuits_selected_df = circuit_df.select(col("circuitId"),col("circuitRef"),col("name"),
                                         col("location"),col("country"),col("lat"),col("lng"),col("alt"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Rename the columns as required

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

circuits_renamed_df = circuits_selected_df\
.withColumnRenamed("circuitId", "circuit_id")\
.withColumnRenamed("circuitRef","circuit_ref")\
.withColumnRenamed("lat","latitude")\
.withColumnRenamed("lng","longitude")\
.withColumnRenamed("alt","altitude")\
.withColumn("data_source",lit(v_data_source))\
.withColumn("file_date",lit(v_file_date))

# COMMAND ----------

#display(circuits_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 4 - Add ingestion date to the dataframe

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit

# COMMAND ----------

circuits_final_df = circuits_renamed_df.withColumn("ingestion_date",current_timestamp())
#.withColumn("env",lit("Dev"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 5 - Write data to datalake as parquet

# COMMAND ----------

#circuits_final_df.write.parquet(f"{processed_folder_path}/circuits",mode="overwrite")
circuits_final_df.write.format("parquet").mode("overwrite").saveAsTable("f1_processed.circuits")

# COMMAND ----------

#%fs
#ls /mt/4mula1dl/processed/circuits

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.circuits;

# COMMAND ----------

#display(dbutils.fs.ls("/mt/4mula1dl/processed/circuits"))

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

