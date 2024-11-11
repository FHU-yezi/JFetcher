-- date: 2024-11-11
-- description: 初始化

CREATE ROLE jfetcher LOGIN PASSWORD 'jfetcher';

CREATE DATABASE jianshu WITH OWNER = jfetcher;
CREATE DATABASE jpep WITH OWNER = jfetcher;

GRANT CONNECT ON DATABASE jianshu TO jfetcher;
GRANT CONNECT ON DATABASE jpep TO jfetcher;