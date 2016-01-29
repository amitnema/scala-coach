--Create schema
create database usa_gov;
use usa_gov;

--dfs -copyFromLocal /usr/local/apache-hive-1.2.1-bin/hcatalog/share/hcatalog/hive-hcatalog-core-1.2.1.jar /home/hduser/apn/projects/hive;
--add jar /home/hduser/apn/projects/hive/hive-hcatalog-core-1.2.1.jar
add jar /usr/lib/hive-hcatalog/share/hcatalog/hive-hcatalog-core.jar;

drop table if exists Clickstr;

--Create table
CREATE TABLE if not exists clickstr (a string, c string, nk int, tz string, gr string, g string, h string,l string, hh string, r string, u string, t int, hc int, cy string, al string) ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe';

--Insert data
LOAD data local inpath '/home/cloudera/apn/temp/Hive_Use_case_example_with_US_government_web_sites_data/UsaGovData.txt' overwrite into table Clickstr;

CREATE EXTERNAL TABLE IF NOT EXISTS clickstrpart (a string, nk int, tz string, gr string, g string, h string,l string, hh string, r string, u string, t bigint, hc bigint, cy string, al string) PARTITIONED BY (c string);


SET hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
INSERT OVERWRITE TABLE clickstrpart PARTITION(c) SELECT a, nk, tz, gr, g, h,l, hh, r, u, t, hc, cy, al, c FROM clickstr;

<li>The top 10 most popular sites in terms of clicks.</li>
SELECT c,u,count(*) as cnt from clickstrpart GROUP BY c,u ORDER BY cnt DESC LIMIT 10;

<li>The top-10 most popular sites for each country</li>
SELECT u,count(*) as cnt from clickstrpart GROUP BY u ORDER BY cnt DESC LIMIT 10;

CREATE EXTERNAL TABLE IF NOT EXISTS clickpartition (a string, nk int,g string, h string,l string,hh string, r string ,u string, t bigint,gr string, cy string, tz string,hc bigint,al string,month int) PARTITIONED BY (c string) row format delimited fields terminated by '\t';

set hive.exec.partition.dynamic=true;
set hive.exec.dynamic.partition.mode=nonstrict;

insert overwrite table clickpartition partition(c) select a,nk,g,h,l,hh,r,u ,t,gr,cy,tz,hc, al,  month(from_unixtime(t)), c from clickstr;

<li>Top-10 most popular sites for each month</li>
SELECT month,count(*) as cnt from clickpartition where month=1 GROUP BY month ORDER BY cnt DESC LIMIT 10;