CREATE TABLE IF NOT EXISTS employee ( eid int, name String,
salary String, destination String)
COMMENT 'Employee details'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE;

ALTER TABLE employee
ADD PARTITION (year='2013')
location '/2012/part2012';

LOAD DATA LOCAL INPATH '/home/hduser/apn/projects/hive/employee/file1' overwrite INTO TABLE employee PARTITION(year='2013');