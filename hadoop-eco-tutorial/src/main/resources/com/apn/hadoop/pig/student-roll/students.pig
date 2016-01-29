--load students
students = LOAD 'students.csv' 
USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'NOCHANGE', 'SKIP_INPUT_HEADER') 
AS (NAME : chararray, AGE : int, GPA : double);
--load rollnumbers
rollnumbers = LOAD 'rollnumbers.csv'
USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'NOCHANGE', 'SKIP_INPUT_HEADER') 
AS (NAME : chararray, ROLLNUM : int);
--*****inner JOIN
in_join = JOIN students by NAME, rollnumbers BY NAME;
--*****left outer join
--SYNTEX : alias = JOIN left-alias BY left-alias-column [LEFT|RIGHT|FULL] [OUTER], right-alias BY right-alias-column [USING 'replicated' | 'skewed'] [PARALLEL n]; 
lo_join = JOIN students by NAME LEFT OUTER, rollnumbers BY NAME;
-- CROSS JOIN
join_stu_rollnums = cross students, rollnumbers;
-- max GPA
B = GROUP students ALL;
C = FOREACH B {
        ord = ORDER students BY GPA DESC;
        top = LIMIT ord 1;
        GENERATE FLATTEN(top);  
};
DUMP C;