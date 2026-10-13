CREATE DATABASE IF NOT EXISTS teta;

CREATE TABLE IF NOT EXISTS teta.transactions_kafka
(
    transaction_time String,
    merch            String,
    cat_id           String,
    amount           String,
    name_1           String,
    name_2           String,
    gender           String,
    street           String,
    one_city         String,
    us_state         String,
    post_code        String,
    lat              String,
    lon              String,
    population_city  String,
    jobs             String,
    merchant_lat     String,
    merchant_lon     String,
    target           String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'transactions',
    kafka_group_name = 'clickhouse-consumer-base',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1,
    kafka_skip_broken_messages = 100;

CREATE TABLE IF NOT EXISTS teta.transactions
(
    transaction_time DateTime,
    merch            String,
    cat_id           String,
    amount           Float64,
    name_1           String,
    name_2           String,
    gender           String,
    street           String,
    one_city         String,
    us_state         String,
    post_code        String,
    lat              Float64,
    lon              Float64,
    population_city  UInt32,
    jobs             String,
    merchant_lat     Float64,
    merchant_lon     Float64,
    target           Float64
)
ENGINE = MergeTree()
ORDER BY (us_state, transaction_time)
PARTITION BY toYYYYMM(transaction_time);

CREATE MATERIALIZED VIEW IF NOT EXISTS teta.transactions_mv TO teta.transactions
AS
SELECT
    parseDateTimeBestEffort(transaction_time) AS transaction_time,
    merch,
    cat_id,
    toFloat64(amount) AS amount,
    name_1,
    name_2,
    gender,
    street,
    one_city,
    us_state,
    post_code,
    toFloat64(lat) AS lat,
    toFloat64(lon) AS lon,
    toUInt32(population_city) AS population_city,
    jobs,
    toFloat64(merchant_lat) AS merchant_lat,
    toFloat64(merchant_lon) AS merchant_lon,
    toFloat64(target) AS target
FROM teta.transactions_kafka;
