-- date: 2025-03-07
-- description: 添加会员信息

CREATE TYPE enum_users_membership_type AS ENUM (
    'NONE',
    'BRONZE',
    'SILVER',
    'GOLD',
    'PLATINA',
    'LEGACY_ORDINARY',
    'LEGACY_DISTINGUISHED'
);

ALTER TABLE users ADD COLUMN membership_type enum_users_membership_type;
UPDATE users SET membership_type = 'NONE';
ALTER TABLE users ALTER COLUMN membership_type SET NOT NULL;

ALTER TABLE users ADD COLUMN membership_expire_time TIMESTAMP;