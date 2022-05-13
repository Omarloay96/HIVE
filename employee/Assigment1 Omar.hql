CREATE DATABASE Assign1;

CREATE DATABASE Assign1_Loc LOCATION '/hp_db/ Assign1_Loc';

CREATE TABLE IF NOT EXISTS assign1.assign1_intern_tab (
 ID int,
 Name string,
 Age int,
 Country string )
 COMMENT 'Employee Table'
 ROW FORMAT DELIMITED
 FIELDS TERMINATED BY ',';

LOAD DATA LOCAL INPATH '/home/data/employee_details.txt' INTO TABLE assign1_intern_tab;

 Select * from assign1_intern_tab;

CREATE EXTERNAL TABLE IF NOT EXISTS assign1_loc.assign1_intern_tab (
 ID int,
 Name string,
 Age int,
 Country string )
 COMMENT 'Sec Employee Table'
 ROW FORMAT DELIMITED
 FIELDS TERMINATED BY ',';

DROP TABLE assign1_loc.assign1_intern_tab; 

DROP TABLE assign1.assign1_intern_tab;  

CREATE TABLE IF NOT EXISTS assign1.assign1_intern_tab (
 ID int,
 Name string,
 Age int,
 Country string )
 COMMENT 'Employee Table'
 ROW FORMAT DELIMITED
 FIELDS TERMINATED BY ',';

CREATE EXTERNAL TABLE IF NOT EXISTS assign1_loc.assign1_intern_tab (
 ID int,
 Name string,
 Age int,
 Country string )
 COMMENT 'Sec Employee Table'
 ROW FORMAT DELIMITED
 FIELDS TERMINATED BY ',';

CREATE TABLE IF NOT EXISTS assign1.staging (
 ID int,
 Name string,
 Age int,
 Country string )
 COMMENT 'STAGING'
 ROW FORMAT DELIMITED
 FIELDS TERMINATED BY ',';

LOAD DATA  INPATH '/course_demo/employee_details.txt' INTO TABLE assign1.staging ;

INSERT INTO assign1.assign1_intern_tab select * from assign1.staging ;
INSERT INTO assign1_loc.assign1_intern_tab select * from assign1.staging ;

! wc -l /employee/songs.csv ;

CREATE TABLE assign1.songs(
   Artist_id STRING,
    Artist_latitude DOUBLE ,
    Artist_location STRING,
    Artist_longtitude DOUBLE,
    Artist_name STRING,
    Duration DOUBLE,
    Num_songs INT ,
    Song_id STRING,
    Title STRING,
    Year STRING)
    ROW FORMAT DELIMITED
     FIELDS TERMINATED BY ',' ;

LOAD DATA  LOCAL INPATH '/employee/songs.csv' INTO TABLE assign1.songs ;
SELECT * FROM assign1.songs
  LIMIT 10 ;

SELECT COUNT(*) FROM assign1.songs ;

CREATE EXTERNAL TABLE assign1.songs_ext(
artist_id STRING,
artist_latitude DOUBLE ,
artist_location STRING,
artist_longtitude DOUBLE,
artist_name STRING,
duration DOUBLE,
num_songs INT ,
song_id STRING,
title STRING,
year STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde';

!hdfs dfs -put /employee/songs.csv /user/hive/warehouse/assign1.db/songs_ext ;
hive -e 'SELECT * FROM assign1.staging;'

Create_table.hql:
DROP TABLE IF EXISTS assign1.assign1_intern_tab ; 
use assign1;
CREATE  TABLE IF NOT EXISTS assign1.assign1_intern_tab(
ID INT ,
NAME VARCHAR(20),
AGE INT,
COUNTRY VARCHAR(20))
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' ;
LOAD DATA LOCAL INPATH '/employee/employee_details.txt' INTO TABLE assign1_intern_tab;

hive -f create_table.hql

ALTER TABLE assign1_intern_tab RENAME TO  testdb.assign1_intern_tab;
