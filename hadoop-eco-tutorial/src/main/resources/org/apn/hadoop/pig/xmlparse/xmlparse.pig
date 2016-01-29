--register '/usr/local/pig-0.15.0/lib/piggybank.jar';

--A = load 'temp/pig_projects/xmlparse/reviews.xml' using org.apache.pig.piggybank.storage.XMLLoader('document') as (data:chararray);
--B = foreach A GENERATE FLATTEN(REGEX_EXTRACT_ALL(data,'(?s)<document>.*?<url>([^>]*?)</url>.*?<category>([^>]*?)</category>.*?<usercount>([^>]*?)</usercount>.*?<reviews>.*?<review>\\s*([^>]*?)\\s*</review>.*?</reviews>.*?</document>')) as (url:chararray,catergory:chararray,usercount:int,review:chararray);

---*************** RegEx Way
--A = LOAD 'temp/pig_projects/xmlparse/reviews.xml' using org.apache.pig.piggybank.storage.XMLLoader('document') as (x:chararray);
--B = FOREACH A GENERATE FLATTEN(REGEX_EXTRACT_ALL(x   ,'(?s)<document>.*?<url>([^>]*?)</url>.*?<category>([^>]*?)</category>.*?<usercount>([^>]*?)</usercount>.*?</document>')) as (url:chararray,catergory:chararray,usercount:int);
--C = LOAD 'temp/pig_projects/xmlparse/reviews.xml' using org.apache.pig.piggybank.storage.XMLLoader('review') as (review:chararray);
--D = FOREACH C GENERATE FLATTEN(REGEX_EXTRACT_ALL(review,'<review>([^>]*?)</review>'));
--E = cross B,D;
--dump E;

---*************** XPath Way:
DEFINE XPath org.apache.pig.piggybank.evaluation.xml.XPath();
A = LOAD 'reviews.xml' using org.apache.pig.piggybank.storage.XMLLoader('document') as (x:chararray);
B = FOREACH A GENERATE XPath(x, 'document/url'), XPath(x, 'document/category'), XPath(x, 'document/usercount');
C = LOAD 'reviews.xml' using org.apache.pig.piggybank.storage.XMLLoader('review') as (review:chararray);
D = FOREACH C GENERATE XPath(review,'review');
E = cross B,D;
dump E;
mkdir output;
rm output;
STORE E INTO 'output';