CREATE TABLE assign2.events_mg(
 Artist STRING, Auth STRING, Firstname STRING , Gender STRING, itemInSession STRING, Lastname STRING, Length DOUBLE , Level STRING, Location STRING, Method STRING, Page STRING, Registration STRING,  SessionId INT,  Song STRING, Status INT, Ts STRING, Useragent STRING, UserId INT)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde';

       LOAD DATA LOCAL INPATH '/employee/events.csv' INTO TABLE assign2.events_mg ;

SELECT UserID , SessionId , FIRST_VALUE(song)OVER(PARTITION BY SessionId ORDER BY Ts
ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as First_Song ,
LAST_VALUE(song)OVER(PARTITION BY SessionId ORDER BY Ts  ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as Last_Song
FROM events_mg ;

SELECT x.userId , RANK()OVER(ORDER BY x.count_distinct) as Ranks
FROM  (SELECT userId , COUNT(DISTINCT song) as count_distinct
FROM events_mg
GROUP BY UserId )x ;

SELECT x.UserId , ROW_NUMBER() OVER(ORDER BY x.count_distinct) as Ranks
 FROM (SELECT userId , COUNT(DISTINCT song) as count_distinct
 FROM events_mg WHERE page='NextSong'
                GROUP BY userId)x;

SELECT Location, Artist , COUNT(song) as count_songs FROM events_mg
 GROUP BY Location, Artist
GROUPING SETS ((Location, Artist),Location,()) ;

SELECT Location, Artist , COUNT(song) as count_songs FROM events_mg
                GROUP BY Location, Artist
  GROUPING SETS ((Location, Artist), Location, Artist, ()) ;

SELECT userId , song , LEAD(song,1) OVER (PARTITION BY UserId ORDER BY Ts) as NEXT_SONG, LAG(song,0) OVER (PARTITION BY UserId ORDER BY Ts) as PREV_SONG
FROM events_mg
 WHERE page='NextSong' ;

SELECT UserId , Song , Ts  FROM events_mg
 ORDER BY UserId , Song ,Ts ;

SELECT UserId , Song , Ts  FROM events_mg
        CLUSTER BY  UserId , Song ,Ts ;
