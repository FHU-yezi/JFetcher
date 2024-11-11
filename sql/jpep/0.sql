-- date: 2024-11-11
-- description: 初始化

GRANT SELECT, INSERT ON TABLE credit_records TO jfetcher;
GRANT INSERT ON TABLE ftn_macket_records TO jfetcher;
GRANT SELECT, INSERT, UPDATE ON TABLE ftn_orders TO jfetcher;
GRANT SELECT, INSERT, UPDATE ON TABLE users TO jfetcher;