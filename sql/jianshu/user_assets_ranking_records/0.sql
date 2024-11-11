-- date: 2024-11-11
-- description: 初始化

CREATE TABLE user_assets_ranking_records (
    date DATE NOT NULL,
    ranking SMALLINT NOT NULL,
    slug VARCHAR(12),
    fp NUMERIC,
    ftn NUMERIC,
    assets NUMERIC,
    CONSTRAINT pk_user_assets_ranking_records_date_ranking PRIMARY KEY (date, ranking)
);