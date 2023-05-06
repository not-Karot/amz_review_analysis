CREATE EXTERNAL TABLE IF NOT EXISTS input_table (
    user_id STRING,
    helpfulness_numerator INT,
    helpfulness_denominator INT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION 'path/to/your/csvfile';

CREATE TABLE IF NOT EXISTS result_table (
    user_id STRING,
    avg_utility DOUBLE);

INSERT OVERWRITE TABLE result_table
SELECT
    user_id,
    AVG(CASE
        WHEN helpfulness_denominator != 0 THEN helpfulness_numerator / helpfulness_denominator
        ELSE 0
    END) as avg_utility
FROM
    input_table
GROUP BY
    user_id
ORDER BY
    avg_utility DESC;
