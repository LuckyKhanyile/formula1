# Databricks notebook source
dbutils.notebook.help()

# COMMAND ----------

v_result = dbutils.notebook.run("1.ingest_circuits_file",0,{"p_data_source":"Ergast API", "v_date_file":"2021-03-28"})

# COMMAND ----------

v_result = dbutils.notebook.run("2.ingest_races_file",0,{"p_data_source":"Ergast API","v_date_file":"2021-03-28"})
v_result

# COMMAND ----------

v_result = dbutils.notebook.run("3.ingest_constructors_life",0,{"p_data_source":"Ergast API", "v_date_file":"2021-03-28"})
v_result

# COMMAND ----------

v_result = dbutils.notebook.run("4.ingest_drivers_file",0,{"p_data_source":"Ergast API" , "v_date_file":"2021-03-28"})
v_result

# COMMAND ----------

v_result = dbutils.notebook.run("5.ingest_results_file",0,{"p_data_source":"Ergast API","v_date_file":"2021-03-28"})
v_result

# COMMAND ----------

v_result = dbutils.notebook.run("6.ingest_qualifying_file",0,{"p_data_source":"Ergast API","v_date_file":"2021-03-28"})
v_result

# COMMAND ----------

v_result = dbutils.notebook.run("7.ingest_pitstops_file",0,{"p_data_source":"Ergast API","v_date_file":"2021-03-28"})
v_result

# COMMAND ----------

v_result = dbutils.notebook.run("8.ingest_laptimes_file",0,{"p_data_source":"Ergast API","v_date_file":"2021-03-28"})
v_result

# COMMAND ----------

