-- date: 2025-02-13
-- description: 初始化

CREATE TABLE users_count_records (
    date DATE NOT NULL CONSTRAINT pk_users_count_records_date PRIMARY KEY,
    total_users_count INTEGER NOT NULL
);