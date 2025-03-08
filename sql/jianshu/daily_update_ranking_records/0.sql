-- date: 2024-11-11
-- description: 初始化

CREATE TABLE daily_update_ranking_records (
    date DATE NOT NULL,
    ranking SMALLINT NOT NULL,
    slug VARCHAR(12),
    days SMALLINT,
    CONSTRAINT pk_daily_update_ranking_records_date_slug PRIMARY KEY (date, slug)
);