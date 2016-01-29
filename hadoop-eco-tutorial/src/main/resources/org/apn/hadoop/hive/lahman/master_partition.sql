drop table tmp_master;

create table tmp_master ( lahmanID string, playerID string, managerID string, hofID string, birthYear string, birthMonth string, birthDay string, birthCountry string, birthState string, birthCity string, deathYear string, deathMonth string, deathDay string, deathCountry string, deathState string, deathCity string, nameFirst string, nameLast string, nameNote string, nameGiven string, nameNick string, weight string, height string, bats string, throws string, debut string, finalGame string, college string, lahman40ID string, lahman45ID string, retroID string, holtzID string, bbrefID string ) CLUSTERED BY (birthCity) SORTED BY (birthCity ASC) INTO 4 BUCKETS;

load data local inpath '/home/hduser/apn/projects/hive/lahman/tmp_master.csv' into table tmp_master;

--drop  table master;
--create table master (lahmanID int, playerID string, managerID int,hofID string,birthYear int,birthMonth int,birthDay string,birthCity string) partitioned by (birthCountry string, birthState string) CLUSTERED BY (birthCity) SORTED BY (birthCity ASC) INTO 4 BUCKETS;

--create table master (lahmanID int, playerID string, managerID int,hofID string,birthYear int,birthMonth int,birthDay string,birthCity string) partitioned by (birthCountry string, birthState string);

--INSERT OVERWRITE TABLE master PARTITION (birthCountry, birthState) SELECT lahmanID ,playerID ,managerID ,hofID ,birthYear ,birthMonth ,birthDay ,birthCity , birthCountry, birthState FROM tmp_master;


drop  table master;
create table master (lahmanID int, playerID string, managerID int,hofID string,birthYear int,birthMonth int,birthDay string,birthState string, birthCity string) partitioned by (birthCountry string) CLUSTERED BY (birthCity) SORTED BY (birthCity ASC) INTO 4 BUCKETS;

INSERT OVERWRITE TABLE master PARTITION (birthCountry) SELECT lahmanID ,playerID ,managerID ,hofID ,birthYear ,birthMonth ,birthDay , birthState, birthCity ,birthCountry FROM tmp_master;