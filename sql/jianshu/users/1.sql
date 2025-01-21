-- date: 2025-01-21
-- description: 用户 ID、昵称、头像链接不允许为空

ALTER TABLE users ALTER COLUMN id SET NOT NULL;
ALTER TABLE users ALTER COLUMN name SET NOT NULL;
ALTER TABLE users ALTER COLUMN avatar_url SET NOT NULL;