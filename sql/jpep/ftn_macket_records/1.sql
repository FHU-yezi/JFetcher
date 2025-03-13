-- date: 2025-01-23
-- description: 创建 2025 年分区表

CREATE TABLE ftn_market_records_2025 PARTITION OF ftn_market_records 
FOR VALUES FROM ('2025-01-01 00:00:00') TO ('2025-12-31 23:59:59');