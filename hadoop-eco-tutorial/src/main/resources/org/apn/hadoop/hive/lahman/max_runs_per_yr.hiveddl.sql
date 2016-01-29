drop table temp_batting; 
create table temp_batting (col_value string);

LOAD DATA LOCAL INPATH '/home/hduser/apn/projects/hive/lahman/Batting.csv' OVERWRITE INTO TABLE temp_batting;

drop table batting;
create table batting (player_id string, year int, runs int);

insert overwrite table batting  
select  
  regexp_extract(col_value, '^(?:([^,]*)\,?){1}', 1) player_id,  
  regexp_extract(col_value, '^(?:([^,]*)\,?){2}', 1) year,  
  regexp_extract(col_value, '^(?:([^,]*)\,?){9}', 1) run  
from temp_batting;

--SELECT year, max(runs) FROM batting GROUP BY year;

drop table tbl_max_runs_per_yr;
create table tbl_max_runs_per_yr (player_id string, year int, runs int);

insert overwrite table tbl_max_runs_per_yr
SELECT a.player_id, a.year, a.runs from batting a  
JOIN (SELECT year, max(runs) runs FROM batting GROUP BY year ) b  
ON (a.year = b.year AND a.runs = b.runs);

!rm -r output; 
INSERT OVERWRITE LOCAL DIRECTORY 'output' 
select * from tbl_max_runs_per_yr;

--*****	partition
drop table tbl_batting_part_yr;
create table tbl_batting_part_yr (player_id string, runs int) PARTITIONED BY (year int) CLUSTERED BY (player_id) INTO 4 BUCKETS;
set hive.exec.dynamic.partition.mode=strict;
set hive.exec.dynamic.partition=true;
set hive.enforce.bucketing = true;
From tbl_max_runs_per_yr bat
insert overwrite table tbl_batting_part_yr partition(year)
select bat.player_id, bat.runs, bat.year distribute by year;



LOAD DATA LOCAL INPATH 'output/000000_0' OVERWRITE INTO TABLE tbl_batting_part_yr PARTITION (yr=2011) ;

drop table bucketed_batting;
CREATE TABLE bucketed_batting  (player_id string, year int, runs int)
CLUSTERED BY (player_id) SORTED BY (player_id ASC) INTO 4 BUCKETS;

insert overwrite table bucketed_batting
select * from batting;