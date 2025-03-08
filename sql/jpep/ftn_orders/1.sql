-- date: 2025-01-23
-- description: 最后出现时间不允许为空

ALTER TABLE ftn_orders ALTER COLUMN last_seen_time SET NOT NULL;