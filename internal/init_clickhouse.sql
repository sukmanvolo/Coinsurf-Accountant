CREATE DATABASE smartsurf;

USE smartsurf;

CREATE TABLE smartsurf.transactions
(
    `user_id`    UUID,
    `sender_id`  UUID,
    `node_id`    UUID,
    `data`       Int64,
    `millicent` Float64,
    `type` LowCardinality(String),
    `timestamp`  DateTime,
    `date`       Date
) ENGINE = MergeTree() PARTITION BY toYYYYMM(date)
      ORDER BY (date) SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW earnings
            ENGINE = SummingMergeTree()
                ORDER BY date
            POPULATE
            AS
SELECT sum(millicent) as millicent, node_id, toStartOfDay(date) as date
FROM smartsurf.transactions
WHERE type = 'debit'
GROUP BY (node_id, date);

CREATE MATERIALIZED VIEW data_sold
            ENGINE = SummingMergeTree()
                ORDER BY date
            POPULATE
AS
SELECT sum(data) as data, node_id, toStartOfDay(date) as date
FROM smartsurf.transactions
WHERE type = 'debit'
GROUP BY (node_id, date);