-- Databricks notebook source
-- MAGIC %run "../includes/common_configuration"
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.read.parquet(f"{processed_folder_path}/circuits")

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC #### Circuit Table

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###### Create Tables from csv files circuit.csv and race.csv

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_raw;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS f1_raw.circuits
(
  circuitId int, 
  circuitRef string, 
  name string, 
  location string, 
  country string, 
  lat double, 
  lng double, 
  alt int, 
  url string
)

USING csv
OPTIONS (path "/mnt/formula1dldbricksrg/raw/circuits.csv/",header true)

-- COMMAND ----------

SELECT * FROM f1_raw.circuits;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Race Table

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS f1_raw.races
(
  raceId integer,
  year integer,
  round integer,
  circuitId integer,
  name string,
  date date,
  time string,
  url string
)

USING csv
OPTIONS (path "/mnt/formula1dldbricksrg/raw/races.csv/", header true)

-- COMMAND ----------

SELECT * FROM f1_raw.races;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create constructors.json file Table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.constructors;
CREATE TABLE IF NOT EXISTS f1_raw.constructors
(
constructorId integer,
constructorRef string,
name string,
nationality string,
url string
)
USING json
OPTIONS (path "/mnt/formula1dldbricksrg/raw/constructors.json")

-- COMMAND ----------

SELECT * FROM f1_raw.constructors

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### json File but with different schema for name

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS f1_raw.drivers
(
    driverId integer,
    driverRef string,
    number integer,
    code string,
    name struct <forename:string, surname string>,
    dob date,
    nationality string,
    url string
)
USING json
OPTIONS (path "/mnt/formula1dldbricksrg/raw/drivers.json")

-- COMMAND ----------

SELECT * FROM f1_raw.drivers;

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ### Reasult.json Table 

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS f1_raw.results
(
    resultId integer,
    raceId integer,
    driverId integer,
    constructorId integer,
    number integer,
    grid integer,
    position integer,
    positionText string,
    positionOrder integer,
    points float,
    laps integer,
    time string,
    milliseconds integer,
    fastestLap integer,
    rank integer,
    fastestLapTime string,
    fastestLapSpeed float,
    statusId string
)
USING json
OPTIONS (path "/mnt/formula1dldbricksrg/raw/results.json")

-- COMMAND ----------

SELECT * FROM f1_raw.results;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Pit stops multiline.json table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.pit_stops;
CREATE TABLE IF NOT EXISTS f1_raw.pit_stops
(
    raceId integer,
    driverId integer,
    stop string,
    lap integer,
    time string,
    duration string,
    milliseconds integer
)
USING json
OPTIONS (path "/mnt/formula1dldbricksrg/raw/pit_stops.json", multiLine true)


-- COMMAND ----------

SELECT * FROM f1_raw.pit_stops;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Multiple csv Files table

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS f1_raw.lap_times
(
  raceId integer,
  driverId integer,
  lap integer,
  position integer,
  time string,
  milliseconds integer
)
USING csv
OPTIONS (path "/mnt/formula1dldbricksrg/raw/lap_times")

-- COMMAND ----------

SELECT COUNT(*) FROM f1_raw.lap_times;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Multiple json Files qualifying table

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS f1_raw.qualifying
(
qualifyId integer,
raceId integer,
driverId integer,
constructorId integer,
number integer,
position integer,
q1 string,
q2 string,
q3 string
)
USING json
OPTIONS (path "/mnt/formula1dldbricksrg/raw/qualifying",multiLine true)

-- COMMAND ----------

SELECT count(*) FROM f1_raw.qualifying;

-- COMMAND ----------

SELECT * FROM f1_raw.qualifying;

-- COMMAND ----------

DESCRIBE DATABASE f1_raw;