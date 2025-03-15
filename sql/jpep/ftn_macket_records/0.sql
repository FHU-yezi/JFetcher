-- date: 2024-11-11
-- description: 初始化

CREATE TABLE ftn_market_records (
    fetch_time TIMESTAMP NOT NULL,
    id INTEGER NOT NULL,
    price NUMERIC NOT NULL,
    traded_count SMALLINT NOT NULL,
    total_amount INTEGER NOT NULL,
    traded_amount INTEGER NOT NULL,
    remaining_amount INTEGER NOT NULL,
    minimum_trade_amount INTEGER NOT NULL,
    CONSTRAINT pk_ftn_market_records_fetch_time_id PRIMARY KEY (fetch_time, id)
) PARTITION BY RANGE (fetch_time);

CREATE TABLE ftn_market_records_2023 PARTITION OF ftn_market_records 
FOR VALUES FROM ('2023-01-01 00:00:00') TO ('2023-12-31 23:59:59');

CREATE TABLE ftn_market_records_2024 PARTITION OF ftn_market_records 
FOR VALUES FROM ('2024-01-01 00:00:00') TO ('2024-12-31 23:59:59');