CREATE TABLE batch_table(
    f1 string,
    f2 string,
    f3 string,
    f4 string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
STORED AS SEQUENCEFILE