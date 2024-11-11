-- date: 2024-11-11
-- description: 初始化

CREATE TYPE enum_users_status AS ENUM ('NORMAL', 'INACCESSIBLE');

CREATE TABLE users (
    slug VARCHAR(12) CONSTRAINT pk_users_slug PRIMARY KEY,
    status enum_users_status NOT NULL,
    update_time TIMESTAMP NOT NULL,
    id INTEGER,
    name VARCHAR(15),
    history_names VARCHAR(15)[] NOT NULL,
    avatar_url TEXT
);