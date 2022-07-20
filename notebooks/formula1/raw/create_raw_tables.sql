-- Databricks notebook source
-- MAGIC %md
-- MAGIC ##### Create the circuits.csv table

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_raw;

-- COMMAND ----------

describe database extended f1_raw;

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.circuits;
CREATE TABLE IF NOT EXISTS f1_raw.circuits(
circuitId INT,
circuitRef STRING,
name STRING,
location STRING,
country STRING,
lat DOUBLE,
lng DOUBLE,
alt INT,
url STRING
) using csv
OPTIONS(path "/mnt/4mula1dl/raw/circuits.csv", header True);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Create the races.csv table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.races;
CREATE TABLE IF NOT EXISTS f1_raw.races(
raceId INT,
year INT,
round INT,
circuitId INT,
name STRING,
date STRING,
time STRING,
url STRING
) using csv
OPTIONS(path "/mnt/4mula1dl/raw/circuits.csv", header True);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Create constructor.json table (JSON single line)

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.constructors;
CREATE TABLE IF NOT EXISTS f1_raw.constructors
(
constructorId INT, 
constructorRef String, 
name String, 
nationality String, 
url String)
USING JSON 
OPTIONS(path "/mnt/4mula1dl/raw/constructors.json");

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Create drivers.json table (JSON single line, complex structure)

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.drivers;
CREATE TABLE IF NOT EXISTS f1_raw.drivers (
driverId INT, driverRef STRING, number INT, code STRING, 
name STRUCT<forname: STRING, surname: STRING>,
dob DATE, nationality STRING, url STRING
) USING JSON
OPTIONS(path "/mnt/4mula1dl/raw/drivers.json");

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Create results.json table (JSON single line)

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.results;
CREATE TABLE IF NOT EXISTS f1_raw.results (
resultId INT,
raceId INT,
driverId INT,
constructorId   INT,
number   INT,
grid   INT,
position   INT,
positionText STRING,
positionOrder   INT,
points FLOAT,
laps   INT,
time STRING,
milliseconds   INT,
fastestLap   INT,
rank   INT,
fastestLapTime STRING,
fastestLapSpeed FLOAT,
statusId STRING
) USING JSON
OPTIONS(path "/mnt/4mula1dl/raw/results.json");

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Create pitstops.json table (JSON single line)

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.pit_stops;
CREATE TABLE IF NOT EXISTS f1_raw.pit_stops (
raceId INT, driverId INT, stop INT, lap INT, time STRING, duration STRING, milliseconds INT)
USING JSON
OPTIONS(path "/mnt/4mula1dl/raw/pit_stops.json", multiLine true);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create table for list of files

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Create lap_times table (for CSV files)

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.lap_times;
CREATE TABLE IF NOT EXISTS f1_raw.lap_times (
raceId INT, 
driverId INT, 
lap INT, 
position INT,
time STRING, 
milliseconds INT
)
USING CSV
OPTIONS(path "/mnt/4mula1dl/raw/lap_times");

-- COMMAND ----------

select count(*) from f1_raw.lap_times

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Create qualifying table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.qualifying;
CREATE TABLE IF NOT EXISTS f1_raw.qualifying (
qualifyId INT,
raceId INT,
driverId INT, 
constructorId INT,
number INT, 
position INT,
q1 STRING,
q2 STRING,
q3 STRING
)
USING JSON
OPTIONS(path "/mnt/4mula1dl/raw/qualifying", multiLine true);

-- COMMAND ----------

select *  from f1_raw.qualifying

-- COMMAND ----------

