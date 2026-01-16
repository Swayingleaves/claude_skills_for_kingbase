# KingbaseES SQL 语法规范

本文档描述 KingbaseES (人大金仓) 数据库的 SQL 语法规范和最佳实践。

## 数据类型

### 数值类型

| 类型 | 描述 | 存储大小 | 范围 |
|------|------|----------|------|
| `SMALLINT` | 小范围整数 | 2字节 | -32768 到 +32767 |
| `INTEGER` / `INT` | 整数 | 4字节 | -2147483648 到 +2147483647 |
| `BIGINT` | 大整数 | 8字节 | -9223372036854775808 到 +9223372036854775807 |
| `DECIMAL(p,s)` | 精确数值 | 可变 | 用户指定精度和小数位数 |
| `NUMERIC(p,s)` | 精确数值 | 可变 | 同 DECIMAL |
| `REAL` | 单精度浮点 | 4字节 | 6位十进制精度 |
| `DOUBLE PRECISION` | 双精度浮点 | 8字节 | 15位十进制精度 |

### 字符串类型

| 类型 | 描述 | 存储大小 |
|------|------|----------|
| `VARCHAR(n)` | 变长字符串 | 最大 n 字符 |
| `CHAR(n)` / `CHARACTER(n)` | 定长字符串 | n 字符 |
| `TEXT` | 变长文本 | 无限制 |

### 日期时间类型

| 类型 | 描述 | 存储大小 |
|------|------|----------|
| `DATE` | 日期 (年-月-日) | 4字节 |
| `TIME` | 时间 (时:分:秒) | 8字节 |
| `TIMESTAMP` | 日期和时间 | 8字节 |
| `TIMESTAMPTZ` | 带时区的日期时间 | 8字节 |

### 二进制类型

| 类型 | 描述 |
|------|------|
| `BYTEA` | 变长二进制数据 |

## DDL 语句

### 创建数据库

```sql
CREATE DATABASE database_name
    [ WITH ]
    [ OWNER = user_name ]
    [ ENCODING = encoding ]
    [ LC_COLLATE = collation ]
    [ LC_CTYPE = ctype ]
    [ TABLESPACE = tablespace_name ];
```

示例:
```sql
CREATE DATABASE mydb
    WITH
    OWNER = system
    ENCODING = 'UTF8'
    LC_COLLATE = 'en_US.UTF-8'
    LC_CTYPE = 'en_US.UTF-8';
```

### 创建表

```sql
CREATE TABLE table_name (
    column_name data_type [column_constraint],
    ...
    [table_constraint]
);
```

示例:
```sql
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    username VARCHAR(50) NOT NULL UNIQUE,
    email VARCHAR(100) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR(20) DEFAULT 'active'
);

CREATE TABLE orders (
    order_id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
    total_amount DECIMAL(10,2) NOT NULL,
    order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR(20) DEFAULT 'pending'
);
```

### 修改表

```sql
-- 添加列
ALTER TABLE table_name ADD COLUMN column_name data_type;

-- 删除列
ALTER TABLE table_name DROP COLUMN column_name;

-- 修改列类型
ALTER TABLE table_name ALTER COLUMN column_name TYPE data_type;

-- 重命名列
ALTER TABLE table_name RENAME COLUMN old_name TO new_name;

-- 添加约束
ALTER TABLE table_name ADD CONSTRAINT constraint_name UNIQUE (column_name);

-- 删除约束
ALTER TABLE table_name DROP CONSTRAINT constraint_name;
```

### 删除表

```sql
DROP TABLE table_name [ CASCADE ];
```

## DML 语句

### INSERT

```sql
INSERT INTO table_name (column1, column2, ...)
VALUES (value1, value2, ...);

-- 批量插入
INSERT INTO table_name (column1, column2, ...)
VALUES
    (value1, value2, ...),
    (value3, value4, ...),
    ...;

-- 从查询插入
INSERT INTO table_name (column1, column2, ...)
SELECT column1, column2 FROM another_table;
```

### SELECT

```sql
SELECT column1, column2, ...
FROM table_name
WHERE condition
GROUP BY column1
HAVING aggregate_condition
ORDER BY column1 [ASC|DESC]
LIMIT count;
```

### UPDATE

```sql
UPDATE table_name
SET column1 = value1, column2 = value2, ...
WHERE condition;
```

### DELETE

```sql
DELETE FROM table_name
WHERE condition;
```

## 约束

### PRIMARY KEY

```sql
CREATE TABLE example (
    id INTEGER PRIMARY KEY,
    ...
);

-- 或
CREATE TABLE example (
    id INTEGER,
    ...
    CONSTRAINT pk_example PRIMARY KEY (id)
);
```

### FOREIGN KEY

```sql
CREATE TABLE example (
    user_id INTEGER REFERENCES users(id)
    ON DELETE CASCADE
    ON UPDATE SET NULL
);

-- 或
CREATE TABLE example (
    user_id INTEGER,
    ...
    CONSTRAINT fk_user FOREIGN KEY (user_id)
    REFERENCES users(id)
);
```

### UNIQUE

```sql
CREATE TABLE example (
    email VARCHAR(100) UNIQUE
);

-- 或
CREATE TABLE example (
    email VARCHAR(100),
    ...
    CONSTRAINT uq_email UNIQUE (email)
);
```

### CHECK

```sql
CREATE TABLE example (
    age INTEGER CHECK (age >= 18)
);
```

### NOT NULL

```sql
CREATE TABLE example (
    name VARCHAR(50) NOT NULL
);
```

## 索引

### 创建索引

```sql
-- 普通索引
CREATE INDEX idx_name ON table_name (column_name);

-- 唯一索引
CREATE UNIQUE INDEX idx_name ON table_name (column_name);

-- 复合索引
CREATE INDEX idx_name ON table_name (column1, column2);

-- 表达式索引
CREATE INDEX idx_name ON table_name (LOWER(column_name));
```

### 删除索引

```sql
DROP INDEX index_name;
```

## 常用函数

### 字符串函数

```sql
-- 拼接
CONCAT(str1, str2, ...) 或 str1 || str2

-- 大小写转换
LOWER(string)
UPPER(string)

-- 子串
SUBSTRING(string FROM start [FOR length])

-- 去除空格
TRIM([LEADING|TRAILING|BOTH] FROM string)

-- 长度
LENGTH(string)

-- 替换
REPLACE(string, from, to)
```

### 数值函数

```sql
-- 四舍五入
ROUND(number, decimals)

-- 向上取整
CEIL(number)

-- 向下取整
FLOOR(number)

-- 绝对值
ABS(number)
```

### 日期时间函数

```sql
-- 当前日期
CURRENT_DATE

-- 当前时间
CURRENT_TIME

-- 当前时间戳
CURRENT_TIMESTAMP

-- 日期加减
date + INTERVAL '1 day'
date - INTERVAL '1 month'

-- 日期差
date1 - date2
```

### 聚合函数

```sql
COUNT(*)
COUNT(column)
SUM(column)
AVG(column)
MIN(column)
MAX(column)
```

## JOIN 类型

```sql
-- 内连接
SELECT * FROM table1
INNER JOIN table2 ON table1.id = table2.id;

-- 左连接
SELECT * FROM table1
LEFT JOIN table2 ON table1.id = table2.id;

-- 右连接
SELECT * FROM table1
RIGHT JOIN table2 ON table1.id = table2.id;

-- 全连接
SELECT * FROM table1
FULL OUTER JOIN table2 ON table1.id = table2.id;
```

## 事务控制

```sql
-- 开始事务
BEGIN;

-- 提交
COMMIT;

-- 回滚
ROLLBACK;

-- 保存点
SAVEPOINT savepoint_name;
ROLLBACK TO SAVEPOINT savepoint_name;
RELEASE SAVEPOINT savepoint_name;
```

## 权限管理

```sql
-- 授权
GRANT privilege ON object TO user;

-- 授权示例
GRANT SELECT, INSERT ON TABLE users TO app_user;
GRANT ALL PRIVILEGES ON DATABASE mydb TO admin;

-- 撤销权限
REVOKE privilege ON object FROM user;

-- 授予模式权限
GRANT USAGE ON SCHEMA schema_name TO user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA schema_name TO user;
```
