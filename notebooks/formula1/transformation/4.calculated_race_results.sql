-- Databricks notebook source
use f1_processed;

-- COMMAND ----------

create table if not exists f1_presentation.calculated_race_results using parquet as
select 
races.race_year,
constructors.name as team_name,
drivers.name as driver_name,
results.position,
results.points,
11-results.position as calculated_points
from  results
inner join drivers on (drivers.driver_id =  results.driver_id)
inner join constructors on (results.constructor_id = constructors.constructor_id)
inner join races on (results.race_id = races.race_id)
where results.position<=10


-- COMMAND ----------

select * from 

-- COMMAND ----------

