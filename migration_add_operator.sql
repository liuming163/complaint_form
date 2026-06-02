-- 登录系统：给 complaints 表添加 operator 字段
ALTER TABLE complaints ADD COLUMN operator VARCHAR(100) DEFAULT '' COMMENT '操作人用户名' AFTER submitted_at;
