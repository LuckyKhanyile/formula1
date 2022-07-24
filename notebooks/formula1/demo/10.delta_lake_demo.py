# Databricks notebook source
# MAGIC %sql
# MAGIC drop database f1_demo2

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS f1_demo
# MAGIC LOCATION '/mnt/4mula1dl/demo';

# COMMAND ----------

results_df = spark.read.option("inferSchema", True).json("/mnt/4mula1dl/raw/2021-03-28/results.json")

# COMMAND ----------

results_df.write.format("delta").mode("overwrite").saveAsTable("f1_demo.results_managed")

# COMMAND ----------

results_df.write.format("delta").mode("overwrite").partitionBy("constructorId").saveAsTable("f1_demo.results_managed_partitioned")

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW PARTITIONS f1_demo.results_managed_partitioned

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.results_managed;

# COMMAND ----------

results_df.write.format("delta").mode("overwrite").save("/mnt/4mula1dl/demo/results_external")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/4mula1dl/demo

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE f1_demo.results_external
# MAGIC USING DELTA
# MAGIC LOCATION "/mnt/4mula1dl/demo/results_external" 

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.results_external

# COMMAND ----------

results_external_demo = spark.read.format("delta").load("/mnt/4mula1dl/demo/results_external")

# COMMAND ----------


display(results_external_demo)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.results_managed where where position<=10 order by length(position),position

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE f1_demo.results_managed
# MAGIC SET points = 11-position
# MAGIC where position<=10

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.results_managed where where position<=10 order by length(position),position

# COMMAND ----------

from delta.tables import DeltaTable
deltaTable = DeltaTable.forPath(spark, "/mnt/4mula1dl/demo/results_managed")
deltaTable.update("position<=10",{"points" : "20-position"})

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.results_managed where where position<=10 order by length(position),position

# COMMAND ----------

