set test.table_name=set_table;

CREATE TABLE ${hiveconf:test.table_name}(
    f1 string,
    f2 string,
    f3 string,
    f4 string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
STORED AS SEQUENCEFILE;