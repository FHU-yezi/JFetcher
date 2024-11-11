-- date: 2024-11-11
-- description: 初始化

CREATE TABLE users (
    id INTEGER NOT NULL CONSTRAINT pk_users_id PRIMARY KEY,
    update_time TIMESTAMP NOT NULL,
    name TEXT NOT NULL,
    hashed_name VARCHAR(9) NOT NULL,
    avatar_url TEXT
);