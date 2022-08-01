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

deltaTable.delete("position=0")

# COMMAND ----------

from pyspark.sql.functions import upper

drivers_day1_df = spark.read.option("inferSchema","True").json("/mnt/4mula1dl/raw/2021-03-28/drivers.json").filter("driverId<=10")\
                            .select("driverId","dob","name.forename","name.surname") 

# COMMAND ----------

display(drivers_day1_df)

# COMMAND ----------

drivers_day1_df.createOrReplaceTempView("drivers_day1")

# COMMAND ----------

from pyspark.sql.functions import upper

drivers_day2_df = spark.read.option("inferSchema","True").json("/mnt/4mula1dl/raw/2021-03-28/drivers.json").filter("driverId BETWEEN 6 AND 15")\
                            .select("driverId","dob",upper("name.forename").alias("forename"),upper("name.surname").alias("surname")) 

# COMMAND ----------

drivers_day2_df.createOrReplaceTempView("drivers_day2")

# COMMAND ----------

display(drivers_day2_df)

# COMMAND ----------

from pyspark.sql.functions import upper

drivers_day3_df = spark.read.option("inferSchema","True").json("/mnt/4mula1dl/raw/2021-03-28/drivers.json")\
                            .filter("driverId BETWEEN 1 AND 5 OR driverId BETWEEN 16 AND 20")\
                            .select("driverId","dob",upper("name.forename").alias("forename"),upper("name.surname").alias("surname")) 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS f1_demo.drivers_merge(
# MAGIC driverId INT,
# MAGIC dob DATE,
# MAGIC forename STRING,
# MAGIC surname STRING,
# MAGIC createdDate DATE,
# MAGIC updatedDate Date
# MAGIC )
# MAGIC USING DELTA

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO f1_demo.drivers_merge tgt
# MAGIC USING drivers_day1 upd
# MAGIC ON upd.driverId = tgt.driverId
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET tgt.dob = upd.dob,
# MAGIC              tgt.forename = upd.forename,
# MAGIC              tgt.surname = upd.surname,
# MAGIC              tgt.updatedDate = current_timestamp
# MAGIC WHEN NOT MATCHED
# MAGIC THEN INSERT (driverId,dob,forename,surname,createdDate) VALUES (driverId,dob,forename,surname,current_timestamp)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO f1_demo.drivers_merge tgt
# MAGIC USING drivers_day2 upd
# MAGIC ON upd.driverId = tgt.driverId
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET tgt.dob = upd.dob,
# MAGIC              tgt.forename = upd.forename,
# MAGIC              tgt.surname = upd.surname,
# MAGIC              tgt.updatedDate = current_timestamp
# MAGIC WHEN NOT MATCHED
# MAGIC THEN INSERT (driverId,dob,forename,surname,createdDate) VALUES (driverId,dob,forename,surname,current_timestamp)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.drivers_merge

# COMMAND ----------

from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark, '/mnt/4mula1dl/demo/drivers_merge')


deltaTable.alias('tgt') \
  .merge(
    drivers_day3_df.alias('upd'),
    'tgt.driverId = upd.driverId'
  ) \
  .whenMatchedUpdate(set =
    {
             "dob" : "upd.dob",
             "forename" : "upd.forename",
             "surname" : "upd.surname",
             "updatedDate" : "current_timestamp()"
    }
  ) \
  .whenNotMatchedInsert(values =
    {
         "driverId":"upd.driverId",
         "dob" : "upd.dob",
         "forename" : "upd.forename",
         "surname" : "upd.surname",
         "createdDate" : "current_timestamp()"
    }
  ) \
  .execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC desc history f1_demo.drivers_merge;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.drivers_merge  Version as of "3"

# COMMAND ----------

df = spark.read.format("delta").option("version","1").load("/mnt/4mula1dl/demo/drivers_merge")

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS f1_demo.drivers_txn (
# MAGIC   driverId INT,
# MAGIC   dob DATE,
# MAGIC   forename STRING,
# MAGIC   surname STRING,
# MAGIC   createdDate DATE,
# MAGIC   updatedDate Date
# MAGIC ) USING DELTA

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY f1_demo.drivers_txn;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO f1_demo.drivers_txn
# MAGIC SELECT * from f1_demo.drivers_merge where driverId = 2;

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM f1_demo.drivers_txn
# MAGIC WHERE driverId=1;

# COMMAND ----------

for driver_id in range(5,20):
    spark.sql(f"""INSERT INTO f1_demo.drivers_txn 
                SELECT * from f1_demo.drivers_merge 
                WHERE driverId = {driver_id}""")

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO f1_demo.drivers_txn
# MAGIC SELECT * from f1_demo.drivers_merge;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS f1_demo.drivers_convert_to_delta
# MAGIC (
# MAGIC   driverId INT,
# MAGIC   dob DATE,
# MAGIC   forename STRING,
# MAGIC   surname STRING,
# MAGIC   createdDate DATE,
# MAGIC   updatedDate Date
# MAGIC ) USING PARQUET

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO f1_demo.drivers_convert_to_delta
# MAGIC SELECT * from f1_demo.drivers_merge;

# COMMAND ----------

# MAGIC %sql
# MAGIC CONVERT TO DELTA f1_demo.drivers_convert_to_delta;

# COMMAND ----------

df = spark.table("f1_demo.drivers_convert_to_delta")

# COMMAND ----------

df.write.format("parquet").save("/mnt/4mula1dl/demo/drivers_convert_to_delta_new")

# COMMAND ----------

# MAGIC %sql
# MAGIC CONVERT TO DELTA parquet.`/mnt/4mula1dl/demo/drivers_convert_to_delta_new`;

# COMMAND ----------

