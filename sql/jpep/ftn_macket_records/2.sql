-- date: 2025-03-05
-- description: 重命名交易笔数字段

ALTER TABLE ftn_market_records RENAME COLUMN traded_count TO completed_trades_count;