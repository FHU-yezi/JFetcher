-- date: 2025-03-04
-- description: 初始化

GRANT INSERT ON TABLE ftn_market_records TO jfetcher;
GRANT SELECT, INSERT, UPDATE ON TABLE ftn_orders TO jfetcher;
GRANT SELECT, INSERT, UPDATE ON TABLE users TO jfetcher;