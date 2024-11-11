-- date: 2024-11-11
-- description: 初始化

GRANT INSERT ON TABLE article_earning_ranking_records TO jfetcher;
GRANT INSERT ON TABLE daily_update_ranking_records TO jfetcher;
GRANT INSERT ON TABLE user_assets_ranking_records TO jfetcher;
GRANT SELECT, INSERT, UPDATE ON TABLE users TO jfetcher;