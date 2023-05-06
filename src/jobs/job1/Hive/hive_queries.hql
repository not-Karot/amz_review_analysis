CREATE EXTERNAL TABLE IF NOT EXISTS input_table (
    product_id STRING,
    time INT,
    text STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION './../../../input/reviews_job1_dim_1.csv';

CREATE TABLE IF NOT EXISTS result_table (
    year STRING,
    product_id STRING,
    word STRING,
    count INT);

ADD FILE ./udf.py;
CREATE TEMPORARY FUNCTION clean_text AS 'udf.clean_text';

INSERT OVERWRITE TABLE result_table
SELECT
    FROM_UNIXTIME(time, 'yyyy') AS year,
    product_id,
    word,
    COUNT(*) as count
FROM
    input_table
LATERAL VIEW
    explode(split(clean_text(text), ' ')) clean_text_table AS word
WHERE
    LENGTH(word) >= 4
GROUP BY
    FROM_UNIXTIME(time, 'yyyy'),
    product_id,
    word
ORDER BY
    year,
    product_id,
    count DESC;

