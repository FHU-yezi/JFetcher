-- date: 2025-01-22
-- description: 添加对更多表的 SELECT 权限

GRANT SELECT ON TABLE article_earning_ranking_records TO jfetcher;
GRANT SELECT ON TABLE daily_update_ranking_records TO jfetcher;
GRANT SELECT ON TABLE user_assets_ranking_records TO jfetcher;