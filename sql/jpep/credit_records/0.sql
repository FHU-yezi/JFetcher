-- date: 2024-11-11
-- description: 初始化

CREATE TABLE credit_records (
    time TIMESTAMP NOT NULL,
    user_id INTEGER NOT NULL,
    credit INTEGER NOT NULL,
    CONSTRAINT pk_credit_records_time_user_id PRIMARY KEY (time, user_id)
);