-- date: 2025-03-04
-- description: 初始化

CREATE TABLE users (
    id INTEGER NOT NULL CONSTRAINT pk_users_id PRIMARY KEY,
    update_time TIMESTAMP NOT NULL,
    name TEXT NOT NULL,
    avatar_url TEXT NOT NULL
);