DROP TABLE IF EXISTS test_table1;

CREATE EXTERNAL TABLE IF NOT EXISTS test_table1(
    f1 STRING,
    f2 STRING,
    f3 STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
STORED AS SEQUENCEFILE
LOCATION '${TEST_TABLE1_LOC}'
TBLPROPERTIES ('serialization.null.format' = 'NULL');

DROP TABLE IF EXISTS test_table2;

CREATE EXTERNAL TABLE IF NOT EXISTS test_table2(
    f4 STRING,
    f5 STRING,
    f6 STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
STORED AS SEQUENCEFILE
LOCATION '${TEST_TABLE2_LOC}'
TBLPROPERTIES ('serialization.null.format' = 'NULL');

DROP TABLE IF EXISTS test_table1_2;

CREATE EXTERNAL TABLE IF NOT EXISTS test_table1_2(
    f1 STRING,
    f2 STRING,
    f3 STRING,
    f4 STRING,
    f5 STRING,
    f6 STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
STORED AS SEQUENCEFILE
LOCATION '${TEST_TABLE1_2_LOC}'
TBLPROPERTIES ('serialization.null.format' = 'NULL');

INSERT INTO test_table1_2
SELECT * FROM test_table1 t1 JOIN test_table2 t2 ON (t1.f1=t2.f4);

SELECT * FROM test_table1_2;