DROP TABLE input_docs;
DROP TABLE intermediate_table;
DROP TABLE result_table;

CREATE TABLE input_docs (
    product_id STRING,
    time INT,
    text STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t';

LOAD DATA LOCAL INPATH '/Users/gianlucadilorenzo/Desktop/Universita/ProgettoBigData/amz_review_analysis/src/input/reviews_job1_dim_1.csv'
    OVERWRITE INTO TABLE input_docs;

ADD FILE hdfs:///user/gianlucadilorenzo/udf.py;


CREATE TABLE intermediate_table AS
    SELECT TRANSFORM(input_docs.product_id, input_docs.time, input_docs.text)
        USING 'python3 udf.py' AS product_id, year, word
    FROM input_docs;

CREATE TABLE result_table AS
    SELECT product_id, FROM_UNIXTIME(year, 'yyyy') AS year, word, COUNT(*) as count
    FROM intermediate_table
    GROUP BY product_id, year, word;

SELECT * FROM result_table
         ORDER BY
        year,
        product_id,
        count DESC;

DROP TABLE input_docs;
DROP TABLE intermediate_table;
DROP TABLE result_table;
