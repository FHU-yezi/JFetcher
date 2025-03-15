-- date: 2025-03-15
-- description: 初始化

CREATE TABLE core_user_assets_records (
    time TIMESTAMP NOT NULL,
    slug VARCHAR(12),
    fp NUMERIC,
    ftn NUMERIC,
    assets NUMERIC,
    CONSTRAINT pk_core_user_assets_records_time_slug PRIMARY KEY (time, slug)
);
