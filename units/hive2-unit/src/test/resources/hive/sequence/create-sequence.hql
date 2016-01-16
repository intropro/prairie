CREATE TABLE comment_table(
    f1 string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' -- Comment inside statement
LINES TERMINATED BY '\n'
STORED AS SEQUENCEFILE;
SELECT * from comment_table;