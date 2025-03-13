-- date: 2025-03-04
-- description: 初始化

CREATE TABLE ftn_market_records (
    fetch_time TIMESTAMP NOT NULL,
    id BIGINT NOT NULL,
    price NUMERIC NOT NULL,
    total_amount INTEGER NOT NULL,
    traded_amount INTEGER NOT NULL,
    remaining_amount INTEGER NOT NULL,
    minimum_trade_amount INTEGER NOT NULL,
    maximum_trade_amount INTEGER,
    completed_trades_count SMALLINT NOT NULL,
    CONSTRAINT pk_ftn_market_records_fetch_time_id PRIMARY KEY (fetch_time, id)
) PARTITION BY RANGE (fetch_time);

CREATE TABLE ftn_market_records_2025 PARTITION OF ftn_market_records 
FOR VALUES FROM ('2025-01-01 00:00:00') TO ('2025-12-31 23:59:59');