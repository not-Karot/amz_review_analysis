CREATE TABLE input_docs (
    product_id STRING,
    time INT,
    text STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t';

LOAD DATA LOCAL INPATH './../../../input/reviews_job1_dim_1.csv'
    OVERWRITE INTO TABLE input_docs;

ADD FILE ./udf.py;

CREATE TABLE result_table AS
    SELECT TRANSFORM(input_docs.product_id, input_docs.time, input_docs.text)
        USING 'python3 udf.py' AS product_id, year, word, count
    FROM input_docs;

SELECT * FROM result_table;

DROP TABLE input_docs;
DROP TABLE result_table;
