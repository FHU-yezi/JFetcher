-- date: 2025-03-15
-- description: 初始化

CREATE TYPE enum_ftn_market_summary_records_type AS ENUM ('BUY', 'SELL');

CREATE TABLE ftn_market_summary_records (
    fetch_time TIMESTAMP NOT NULL,
    type enum_ftn_market_summary_records_type NOT NULL,
    best_price NUMERIC NOT NULL,
    total_amount INTEGER NOT NULL,
    traded_amount INTEGER NOT NULL,
    remaining_amount INTEGER NOT NULL,
    CONSTRAINT pk_ftn_market_summary_records_fetch_time_type PRIMARY KEY (fetch_time, type)
);
