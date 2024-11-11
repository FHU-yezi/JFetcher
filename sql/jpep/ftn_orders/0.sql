-- date: 2024-11-11
-- description: 初始化

CREATE TYPE enum_ftn_orders_type AS ENUM ('BUY', 'SELL');

CREATE TABLE ftn_orders (
    id INTEGER CONSTRAINT pk_ftn_orders_id PRIMARY KEY,
    type enum_ftn_orders_type NOT NULL,
    publisher_id INTEGER NOT NULL,
    publish_time TIMESTAMP NOT NULL,
    last_seen_time TIMESTAMP
);