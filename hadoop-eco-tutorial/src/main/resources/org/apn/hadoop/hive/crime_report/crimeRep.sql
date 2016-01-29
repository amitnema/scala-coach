CREATE DATABASE IF NOT EXISTS crime_report;
--hadoop fs -copyFromLocal /home/hduser/apn/projects/hive/crime_report/SFPD_Incidents_-_from_1_January_2003.csv /home/hduser/apn/projects/hive
CREATE TABLE IncidentJson (IncidntNum int, 
                            Category string, 
                            Descript string, 
                            DayOfWeek string, 
                            dDate string, 
                            Ttime string, 
                            PdDistrict string, 
                            Resolution string,
                            Address string, 
                            x string, 
                            y string, 
                            LLocation string, 
                            PdId string) 
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' WITH SERDEPROPERTIES("separatorChar" = "\,","quoteChar" = "\"");

LOAD DATA LOCAL INPATH '/home/hduser/apn/projects/hive/crime_report/SFPD_Incidents_-_from_1_January_2003.csv' OVERWRITE INTO TABLE IncidentJson;

--Now we are again copying data from hive table to hdfs to remove commas inside the description.
INSERT OVERWRITE LOCAL DIRECTORY '/home/hduser/apn/projects/hive/crime_report' SELECT printf("%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s", IncidntNum, Category, Descript, DayOfWeek, dDate, Ttime, PdDistrict, Resolution, Address, x, y, LLocation, PdId) FROM IncidentJson;


sqoop.sh server start
sqoop export --connect jdbc:mysql//localhost/sqoop_export --table crime_incidents --username root -P --export-dir /home/hduser/apn/projects/hive/crime_report/