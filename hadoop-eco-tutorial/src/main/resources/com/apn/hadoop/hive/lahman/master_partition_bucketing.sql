drop database baseball;
create database baseball;
use baseball;

drop table tmp_master;
create table tmp_master 
( lahmanID string, playerID string, managerID string, hofID string, birthYear string, birthMonth string, birthDay string, birthCountry string, 
birthState string, birthCity string, deathYear string, deathMonth string, deathDay string, deathCountry string, deathState string, deathCity string, 
nameFirst string, nameLast string, nameNote string, nameGiven string, nameNick string, weight string, height string, bats string, throws string, debut string, finalGame string, college string, 
lahman40ID string, lahman45ID string, retroID string, holtzID string, bbrefID string ) 
row format SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
tblproperties ("skip.header.line.count"="1");;

load data local inpath '/home/hduser/apn/projects/hive/lahman/Master.csv' into table tmp_master;
select lahmanID , playerID , managerID , hofID , birthYear , birthMonth , birthDay , birthCountry , birthState , birthCity  from tmp_master limit 2; 

drop  table master;
create table master 
(lahmanID string, playerID string, managerID string,hofID string,birthYear string,birthMonth string,birthDay string, birthState string, birthCity string) 
partitioned by (birthCountry string) 
CLUSTERED BY (birthCity) 
SORTED BY (birthCity ASC) INTO 4 BUCKETS 
STORED AS ORC;

INSERT OVERWRITE TABLE master PARTITION (birthCountry) SELECT lahmanID ,playerID ,managerID ,hofID ,birthYear ,birthMonth ,birthDay ,birthState, birthCity , birthCountry FROM tmp_master;
select lahmanID , playerID , managerID , hofID , birthYear , birthMonth , birthDay , birthCountry , birthState , birthCity  from master limit 2; 

DROP INDEX idx_birthCity ON master;
CREATE INDEX idx_birthCity ON TABLE master (birthCity) AS 'COMPACT' WITH DEFERRED REBUILD;
ALTER INDEX idx_birthCity ON master REBUILD;
SHOW FORMATTED INDEX ON master;

DROP INDEX idx_birthState ON master;
CREATE INDEX idx_birthState ON TABLE master (birthState) AS 'BITMAP' WITH DEFERRED REBUILD;
ALTER INDEX idx_birthState ON master REBUILD;
SHOW FORMATTED INDEX ON master;

ALTER TABLE master PARTITION (birthCountry) SET FILEFORMAT ORC;
set hive.vectorized.execution.enabled = true;

explain select lahmanID  from master limit 2;