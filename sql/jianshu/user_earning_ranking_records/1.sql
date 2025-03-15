-- date: 2025-03-08
-- description: slug 字段允许为空

ALTER TABLE user_earning_ranking_records ALTER COLUMN slug DROP NOT NULL;