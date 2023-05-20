DROP TABLE IF EXISTS input_docs;
DROP TABLE IF EXISTS intermediate_table;
DROP TABLE IF EXISTS result_table;

CREATE TABLE input_docs (
    product_id STRING,
    event_time INT,
    text STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
LOCATION "${INPUT}";

ADD FILE "${UDF}";


CREATE TABLE intermediate_table AS
    SELECT TRANSFORM(input_docs.product_id, input_docs.event_time, input_docs.text)
        USING 'python3 udf.py' AS product_id, year, word
    FROM input_docs;

CREATE TABLE result_table AS
    SELECT product_id, FROM_UNIXTIME(year, 'yyyy') AS year, word, COUNT(*) as count
    FROM intermediate_table
    GROUP BY year, product_id
    ORDER BY count DESC
    LIMIT 10;

INSERT OVERWRITE DIRECTORY "${OUTPUT}"
SELECT year, product_id, word, count(*) FROM result_table
         GROUP BY year, product_id, word
         ORDER BY
        count DESC
        LIMIT 5;

DROP TABLE input_docs;
DROP TABLE intermediate_table;
DROP TABLE result_table;
