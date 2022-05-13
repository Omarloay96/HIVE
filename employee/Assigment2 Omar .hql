CREATE DATABASE assign2;

CREATE EXTERNAL TABLE assign2.partitioned_songs(
artist_id STRING,
artist_latitude DOUBLE ,
artist_location STRING,
artist_longtitude DOUBLE,
duration DOUBLE,
num_songs INT ,
song_id STRING,
title STRING)
partitioned by (artist_name STRING, year STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde';

!hdfs dfs -put /employee/songs.csv /user/hive/warehouse/assign2.db/partitioned_songs;

DESCRIBE FORMATTED songs partition(artist_name='AR8IEZO1187B99055E',year='2008');

create external table assign2.staging_songs(
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
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
LOCATION '/data/songs';

from staging_songs  insert overwrite table partitioned_songs partition (artist_name,year)
 select * ;

TRUNCATE TABLE partitioned_songs;

from staging_songs
     insert overwrite table  partitioned_songs partition (artist_name , year)
    select artist_id , artist_latitude , artist_location , artist_longtitude  , duration , num_songs , song_id ,title , artist_name , year ;

CREATE TABLE staging_avro LIKE staging_songs;
ALTER TABLE staging_avro
SET SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe';

CREATE TABLE staging_parquet LIKE staging_songs;
ALTER TABLE staging_parquet
SET SERDE 'parquet.hive.serde.ParquetHiveSerDe';

