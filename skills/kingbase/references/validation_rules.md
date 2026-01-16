# KingbaseES SQL 验证规则

本文档详细说明 SQL 验证模块检查的所有规则和标准。

## 验证类别

验证模块对 SQL 语句进行以下类别的检查：

1. **语法正确性** - 确保语句结构有效
2. **安全检查** - 检测 SQL 注入等安全问题
3. **性能优化** - 识别潜在的性能问题
4. **命名规范** - 检查标识符命名是否符合约定
5. **存在性检查** - 验证表和列是否存在

---

## 1. 语法正确性

### 1.1 空语句检查

**规则**: SQL 语句不能为空

**错误示例**:
```
(空语句或仅包含空白字符)
```

**建议**: 提供有效的 SQL 语句

---

### 1.2 括号平衡

**规则**: 所有开括号必须有匹配的闭括号

**错误示例**:
```sql
SELECT * FROM users WHERE (id = 1 OR id = 2;
```

**建议**: 确保所有括号正确配对

---

### 1.3 引号平衡

**规则**: 所有单引号必须成对出现

**错误示例**:
```sql
INSERT INTO users (name) VALUES ('John;
```

**建议**: 确保所有字符串字面量的引号正确闭合

---

### 1.4 分号结尾

**级别**: INFO

**规则**: 语句应以分号结尾（推荐）

**示例**:
```sql
-- 推荐
SELECT * FROM users;

-- 不推荐
SELECT * FROM users
```

**建议**: 添加分号以符合 SQL 标准

---

### 1.5 SELECT 语句结构

**规则**: SELECT 语句应包含 FROM 子句（子查询除外）

**错误示例**:
```sql
SELECT 1, 2, 3;
```

**建议**: 确保查询结构完整

---

## 2. 安全检查

### 2.1 SQL 注入检测

**严重性**: ERROR

**规则**: 检测常见的 SQL 注入模式

#### 注入模式

| 模式 | 描述 |
|------|------|
| `'; DROP TABLE` | 经典 DROP TABLE 注入 |
| `'; DELETE FROM` | DELETE 注入 |
| `'; EXEC(` | 执行命令注入 |
| `' OR '1'='1` | OR 逻辑注入 |
| `' AND '1'='1` | AND 逻辑注入 |
| `'; --` | 注释符注入 |
| `admin'--` | 认证绕过 |
| `admin'#` | 认证绕过 |
| `' OR 1=1` | 恒真条件注入 |
| `UNION SELECT` | UNION 注入 |

**错误示例**:
```sql
-- 直接拼接用户输入（危险）
SELECT * FROM users WHERE name = '" + userName + "';

-- 如果 userName = "admin' OR '1'='1"
-- 实际执行的 SQL:
SELECT * FROM users WHERE name = 'admin' OR '1'='1';
```

**建议**: 始终使用参数化查询

```sql
-- 安全的参数化查询
cursor.execute("SELECT * FROM users WHERE name = %s", (userName,))
```

---

### 2.2 硬编码凭据

**严重性**: WARNING

**规则**: 不应在 SQL 中硬编码密码

**示例**:
```sql
-- 不推荐
SELECT * FROM users WHERE username = 'admin' AND password = 'secret123';

-- 推荐：使用参数化查询
SELECT * FROM users WHERE username = %s AND password = %s;
```

**建议**: 使用参数化查询处理敏感数据

---

## 3. 性能优化

### 3.1 SELECT * 使用

**严重性**: WARNING

**规则**: 避免使用 `SELECT *`，应明确指定所需列

**不推荐**:
```sql
SELECT * FROM users;
```

**推荐**:
```sql
SELECT id, username, email FROM users;
```

**原因**:
- 可能读取不需要的数据，增加网络传输
- 表结构变更可能导致查询行为变化
- 无法利用覆盖索引

---

### 3.2 LIKE 前导通配符

**严重性**: WARNING

**规则**: 避免使用前导 `%` 的 LIKE 模式

**不推荐**:
```sql
SELECT * FROM users WHERE email LIKE '%@example.com';
```

**推荐**:
```sql
-- 使用反向索引或全文搜索
SELECT * FROM users WHERE email LIKE 'user%@example.com';
-- 或
SELECT * FROM users WHERE position('@example.com' in email) > 0;
```

**原因**: 前导通配符无法使用索引，导致全表扫描

---

### 3.3 WHERE 子句中的函数

**严重性**: WARNING

**规则**: 避免在 WHERE 子句中对列使用函数

**不推荐**:
```sql
SELECT * FROM users WHERE LOWER(email) = 'user@example.com';
SELECT * FROM orders WHERE SUBSTR(order_date, 1, 7) = '2024-01';
```

**推荐**:
```sql
SELECT * FROM users WHERE email = LOWER('user@example.com');
SELECT * FROM orders WHERE order_date >= '2024-01-01' AND order_date < '2024-02-01';
```

**原因**: 函数阻止索引使用，可考虑使用函数索引

---

### 3.4 ORDER BY 序数位置

**严重性**: WARNING

**规则**: 避免使用数字位置指定排序列

**不推荐**:
```sql
SELECT id, name, email FROM users ORDER BY 2, 3;
```

**推荐**:
```sql
SELECT id, name, email FROM users ORDER BY name, email;
```

**原因**: 列位置排序脆弱，列顺序变更会导致错误

---

### 3.5 缺少 LIMIT 子句

**严重性**: INFO

**规则**: SELECT 语句应考虑添加 LIMIT 限制结果集大小

**不推荐**:
```sql
SELECT * FROM large_table;
```

**推荐**:
```sql
SELECT * FROM large_table LIMIT 100;
```

**原因**: 防止返回过多数据，保护性能

---

### 3.6 缺少 WHERE 子句的 DELETE/UPDATE

**严重性**: WARNING

**规则**: DELETE/UPDATE 语句应包含 WHERE 子句

**危险操作**:
```sql
DELETE FROM users;
UPDATE orders SET status = 'cancelled';
```

**安全操作**:
```sql
DELETE FROM users WHERE created_at < '2020-01-01';
UPDATE orders SET status = 'cancelled' WHERE status = 'expired';
```

**建议**: 执行前确认条件正确

---

## 4. 命名规范

### 4.1 标识符命名约定

**严重性**: INFO

**规则**: 推荐使用 snake_case 命名标识符

**不推荐**:
```sql
CREATE TABLE UserProfile (
    UserId INTEGER,
    FirstName VARCHAR(50)
);
```

**推荐**:
```sql
CREATE TABLE user_profile (
    user_id INTEGER,
    first_name VARCHAR(50)
);
```

**命名约定**:
- 表名: 小写 snake_case，使用复数形式 (如 `users`, `order_items`)
- 列名: 小写 snake_case (如 `user_id`, `created_at`)
- 主键: `table_name_id` 或简写 `id`
- 外键: `referenced_table_id`
- 时间戳: `created_at`, `updated_at`
- 布尔值: `is_`, `has_`, `can_` 前缀

---

## 5. 存在性检查

### 5.1 表存在性

**严重性**: ERROR

**规则**: 引用的表必须在数据库中存在

**检查**: 验证所有 FROM、JOIN、INSERT INTO、UPDATE、CREATE TABLE 中引用的表是否存在

**示例**:
```sql
-- 错误：表不存在
SELECT * FROM non_existent_table;
```

**建议**: 检查表名拼写，或先创建表

---

### 5.2 列存在性

**严重性**: ERROR

**规则**: 引用的列必须在对应表中存在

**检查**: 验证 SELECT、WHERE、ORDER BY 等子句中的列是否存在

**示例**:
```sql
-- 错误：列不存在
SELECT non_existent_column FROM users;
```

**建议**: 检查列名拼写，查看表结构确认列名

---

## 验证输出格式

验证结果按严重性分为三个级别：

| 级别 | 符号 | 描述 |
|------|------|------|
| ERROR | ✗ | 必须修复的问题，可能导致执行失败 |
| WARNING | ⚠ | 应该修复的问题，可能影响性能或安全 |
| INFO | ℹ | 建议性改进，提升代码质量 |

示例输出:
```
✗ SQL validation failed - 2 error(s), 1 warning(s)

SECURITY:
  ✗ Potential OR-based injection
     → Use parameterized queries instead of string concatenation

SYNTAX:
  ✗ Unbalanced single quotes
     → Ensure all single quotes are properly closed

PERFORMANCE:
  ⚠ Leading wildcard in LIKE prevents index use
```
