set a=b;
 -- Comment with space before
CREATE TABLE comment_table(
    f1 string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' -- Comment inside statement
LINES TERMINATED BY '\n'
STORED AS SEQUENCEFILE;
-- Comment before comment
--Comment without spaces
-- Comment after comment
INSERT INTO TABLE comment_table VALUES ('1');