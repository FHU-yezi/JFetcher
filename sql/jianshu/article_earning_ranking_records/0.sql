-- date: 2024-11-11
-- description: 初始化

CREATE TABLE article_earning_ranking_records (
    date DATE NOT NULL,
    ranking SMALLINT NOT NULL,
    slug VARCHAR(12),
    title TEXT,
    author_slug VARCHAR(12),
    author_earning NUMERIC NOT NULL,
    voter_earning NUMERIC NOT NULL,
    CONSTRAINT pk_article_earning_ranking_records_date_ranking PRIMARY KEY (date, ranking)
);