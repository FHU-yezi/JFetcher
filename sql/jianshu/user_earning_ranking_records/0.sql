-- date: 2025-02-05
-- description: 初始化

CREATE TYPE enum_user_earning_ranking_records_type AS ENUM ('ALL', 'CREATING', 'VOTING');

CREATE TABLE user_earning_ranking_records (
    date DATE NOT NULL,
    type enum_user_earning_ranking_records_type NOT NULL,
    ranking SMALLINT NOT NULL,
    slug VARCHAR(12),
    total_earning NUMERIC NOT NULL,
    creating_earning NUMERIC NOT NULL,
    voting_earning NUMERIC NOT NULL,
    CONSTRAINT pk_user_earning_ranking_records_date_type_ranking PRIMARY KEY (date, type, ranking)
);