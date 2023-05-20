CREATE TABLE reviews (
    productId STRING,
    userId STRING,
    score INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE;

LOAD DATA INPATH '/user/hive/data/data.txt' INTO TABLE reviews;

CREATE TABLE result AS
SELECT
    TRANSFORM (productId, userId, score)
    USING 'python /path/to/mapper.py'
    AS (userId, productId)
    CLUSTER BY userId
    FROM reviews
    WHERE score >= 4;

INSERT OVERWRITE TABLE result
SELECT
    TRANSFORM (userId, productId)
    USING 'python /path/to/reducer.py'
    AS (user_group, common_products)
    FROM result
    WHERE user_group IS NOT NULL;

