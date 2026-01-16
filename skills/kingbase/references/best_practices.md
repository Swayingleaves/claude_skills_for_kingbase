# KingbaseES 性能优化与最佳实践

本文档提供 KingbaseES 数据库的性能优化建议和最佳实践。

## 目录

1. [查询优化](#查询优化)
2. [索引策略](#索引策略)
3. [表设计](#表设计)
4. [连接管理](#连接管理)
5. [批量操作](#批量操作)
6. [事务管理](#事务管理)
7. [分区表](#分区表)
8. [维护与监控](#维护与监控)

---

## 查询优化

### 只查询需要的列

**避免**:
```sql
SELECT * FROM users;
```

**推荐**:
```sql
SELECT id, username, email FROM users;
```

### 使用 LIMIT 限制结果集

```sql
-- 分页查询
SELECT id, name FROM products
ORDER BY id
LIMIT 20 OFFSET 0;

-- 使用更高效的方式（对于大偏移量）
SELECT id, name FROM products
WHERE id > last_seen_id
ORDER BY id
LIMIT 20;
```

### 避免 SELECT DISTINCT（如果可能）

```sql
-- 效率较低
SELECT DISTINCT user_id FROM orders;

-- 更高效（如果 user_id 是主键或唯一索引）
SELECT user_id FROM users
WHERE id IN (SELECT DISTINCT user_id FROM orders);
```

### 使用 EXISTS 代替 IN（对于子查询）

```sql
-- 效率较低（当子查询结果集大时）
SELECT * FROM users
WHERE id IN (SELECT user_id FROM orders WHERE total > 1000);

-- 更高效
SELECT * FROM users u
WHERE EXISTS (
    SELECT 1 FROM orders o
    WHERE o.user_id = u.id AND o.total > 1000
);
```

### 避免 WHERE 子句中的函数

**避免**:
```sql
SELECT * FROM orders WHERE DATE(created_at) = '2024-01-15';
SELECT * FROM users WHERE LOWER(email) = 'user@example.com';
```

**推荐**:
```sql
SELECT * FROM orders
WHERE created_at >= '2024-01-15' AND created_at < '2024-01-16';

-- 或创建函数索引
CREATE INDEX idx_orders_date ON orders (DATE(created_at));
```

### 使用 UNION ALL 代替 UNION（如果不需要去重）

```sql
-- UNION 会去重，需要额外排序操作
SELECT name FROM employees
UNION
SELECT name FROM contractors;

-- UNION ALL 更快，如果确定没有重复
SELECT name FROM employees
UNION ALL
SELECT name FROM contractors;
```

### 优化 JOIN 顺序

```sql
-- 小表驱动大表（将小表放在前面）
SELECT * FROM small_table s
JOIN large_table l ON s.id = l.small_id;

-- 确保连接列有索引
CREATE INDEX idx_large_small_id ON large_table(small_id);
```

---

## 索引策略

### 为常用查询条件创建索引

```sql
-- WHERE 条件
CREATE INDEX idx_orders_status ON orders(status);
CREATE INDEX idx_orders_date ON orders(created_at);

-- JOIN 列
CREATE INDEX idx_orders_user_id ON orders(user_id);

-- 排序列
CREATE INDEX idx_products_created ON products(created_at DESC);
```

### 复合索引的列顺序

```sql
-- 高选择性列在前
CREATE INDEX idx_orders_user_date ON orders(user_id, created_at);

-- 对于 (user_id = 1 AND created_at > '2024-01-01') 这类查询有效
-- 对于 (created_at > '2024-01-01') 无效（前缀原则）
```

### 唯一索引

```sql
-- 业务唯一约束
CREATE UNIQUE INDEX idx_users_email ON users(email);
CREATE UNIQUE INDEX idx_users_username ON users(username);
```

### 部分索引

```sql
-- 只索引活跃用户
CREATE INDEX idx_active_users ON users(created_at)
WHERE status = 'active';

-- 只索引未完成订单
CREATE INDEX idx_pending_orders ON orders(user_id)
WHERE status = 'pending';
```

### 表达式索引

```sql
-- 支持函数查询
CREATE INDEX idx_users_email_lower ON users(LOWER(email));

-- 现在可以使用索引
SELECT * FROM users WHERE LOWER(email) = 'user@example.com';
```

### 覆盖索引

```sql
-- 包含查询所需的所有列，避免回表
CREATE INDEX idx_orders_covering ON orders(user_id, status, total);
```

### 避免过度索引

- 索引会占用存储空间
- 每个索引都会增加 INSERT/UPDATE/DELETE 的开销
- 定期清理未使用的索引

---

## 表设计

### 选择合适的数据类型

```sql
-- 使用最小的合适类型
CREATE TABLE example (
    id SERIAL,                    -- 自增整数
    status SMALLINT,              -- 只有少量状态值
    priority SMALLINT DEFAULT 0,  -- 优先级 0-10
    is_active BOOLEAN DEFAULT TRUE,
    price DECIMAL(10,2),          -- 精确金额
    created_at TIMESTAMP,         -- 不需要时区
    description TEXT              -- 大文本
);
```

### 规范化 vs 反规范化

**规范化（3NF）**:
- 减少数据冗余
- 避免更新异常
- 适合 OLTP 系统

**反规范化**:
- 减少 JOIN
- 提高查询性能
- 适合读密集场景

### 分区表

```sql
-- 按范围分区
CREATE TABLE orders (
    id SERIAL,
    user_id INTEGER,
    total DECIMAL(10,2),
    created_at TIMESTAMP
) PARTITION BY RANGE (created_at);

-- 创建分区
CREATE TABLE orders_2024_q1 PARTITION OF orders
    FOR VALUES FROM ('2024-01-01') TO ('2024-04-01');

CREATE TABLE orders_2024_q2 PARTITION OF orders
    FOR VALUES FROM ('2024-04-01') TO ('2024-07-01');
```

### 使用约束保证数据完整性

```sql
-- CHECK 约束
CREATE TABLE products (
    id SERIAL PRIMARY KEY,
    price DECIMAL(10,2) CHECK (price >= 0),
    quantity INTEGER CHECK (quantity >= 0),
    email VARCHAR(100) CHECK (email ~* '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$')
);
```

---

## 连接管理

### 使用连接池

```python
# 配置连接池
engine = create_engine(
    'postgresql://user:pass@host/db',
    pool_size=5,
    max_overflow=10,
    pool_timeout=30,
    pool_recycle=3600
)
```

### 及时释放连接

```python
# 使用上下文管理器
with get_connection(config) as conn:
    # 执行操作
    pass
# 连接自动释放
```

### 连接参数优化

```python
config = KingbaseConfig(
    host='localhost',
    port=54321,
    connect_timeout=10,
    # 根据应用调整
)
```

---

## 批量操作

### 批量插入

```python
# 低效 - 逐行插入
for data in records:
    execute_statement(
        "INSERT INTO users (name, email) VALUES (%s, %s)",
        params=(data['name'], data['email'])
    )

# 高效 - 批量插入
execute_batch(
    "INSERT INTO users (name, email) VALUES (%s, %s)",
    params_list=[(r['name'], r['email']) for r in records]
)
```

### 使用 COPY 批量导入

```sql
-- 使用 COPY 命令（最快）
COPY users(name, email) FROM '/path/to/data.csv' DELIMITER ',';
```

### 批量更新

```python
# 批量更新同一列的不同值
UPDATE users SET status = CASE
    WHEN id = 1 THEN 'active'
    WHEN id = 2 THEN 'inactive'
    WHEN id = 3 THEN 'pending'
END WHERE id IN (1, 2, 3);
```

---

## 事务管理

### 保持事务简短

```python
# 避免长时间事务
conn.begin()
try:
    # 快速执行
    execute_statement("INSERT INTO ...")
    execute_statement("UPDATE ...")
    conn.commit()
except:
    conn.rollback()
```

### 避免大事务

```python
# 分批处理大事务
def process_large_batch():
    batch_size = 1000
    offset = 0

    while True:
        conn.begin()
        try:
            execute_statement(
                "UPDATE orders SET status = 'processed' "
                "WHERE processed = false LIMIT %s",
                params=(batch_size,)
            )
            conn.commit()

            if affected_rows < batch_size:
                break
        except:
            conn.rollback()
            break
```

### 使用合适的隔离级别

```sql
-- 读已提交（默认）
SET TRANSACTION ISOLATION LEVEL READ COMMITTED;

-- 可重复读（避免幻读）
SET TRANSACTION ISOLATION LEVEL REPEATABLE READ;

-- 串行化（最高隔离，最低性能）
SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;
```

---

## 维护与监控

### 定期 ANALYZE

```sql
-- 更新统计信息
ANALYZE table_name;

-- 或
VACUUM ANALYZE table_name;
```

### 定期 VACUUM

```sql
-- 回收空间并更新统计
VACUUM ANALYZE table_name;

-- 仅回收不更新的表空间
VACUUM FULL table_name;  -- 需要排他锁
```

### 监控慢查询

```sql
-- 查看当前运行的查询
SELECT pid, now() - query_start as duration, query
FROM pg_stat_activity
WHERE state = 'active'
ORDER BY duration DESC;

-- 配置慢查询日志
ALTER DATABASE mydb SET log_min_duration_statement = 1000; -- 1秒
```

### 查看表大小

```sql
-- 查看表大小
SELECT
    schemaname,
    tablename,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) AS size,
    pg_total_relation_size(schemaname||'.'||tablename) AS bytes
FROM pg_tables
WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
ORDER BY bytes DESC
LIMIT 10;
```

### 查看索引使用情况

```sql
-- 查看未使用的索引
SELECT
    schemaname,
    tablename,
    indexname,
    idx_scan as index_scans
FROM pg_stat_user_indexes
WHERE idx_scan = 0
AND indexname NOT LIKE '%_pkey';
```

### EXPLAIN ANALYZE

```sql
-- 分析查询执行计划
EXPLAIN ANALYZE
SELECT u.name, COUNT(o.id)
FROM users u
JOIN orders o ON u.id = o.user_id
GROUP BY u.name;
```

---

## 其他最佳实践

### 预编译语句

```python
# 使用预编译语句（参数化查询）
cursor.execute(
    "SELECT * FROM users WHERE email = %s AND status = %s",
    (email, status)
)
```

### 避免 N+1 查询

```python
# 低效 - N+1 查询
users = execute_query("SELECT * FROM users")
for user in users:
    orders = execute_query(f"SELECT * FROM orders WHERE user_id = {user['id']}")

# 高效 - 一次查询
users = execute_query("""
    SELECT u.*, o.*
    FROM users u
    LEFT JOIN orders o ON u.id = o.user_id
""")
```

### 使用物化视图

```sql
-- 创建物化视图
CREATE MATERIALIZED VIEW user_order_summary AS
SELECT
    u.id,
    u.name,
    COUNT(o.id) as order_count,
    SUM(o.total) as total_spent
FROM users u
LEFT JOIN orders o ON u.id = o.user_id
GROUP BY u.id, u.name;

-- 刷新物化视图
REFRESH MATERIALIZED VIEW user_order_summary;
```
