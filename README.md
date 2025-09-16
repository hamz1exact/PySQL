# PySQL

**PySQL** is a lightweight, in-memory SQL-like database engine implemented in Python. It supports core SQL features including schema creation, CRUD operations, aggregate functions, and advanced `WHERE` filtering. Designed primarily for **learning and experimentation**, it provides insight into how databases parse, plan, and execute queries.

---

## Features

- **Schema Management**  
  - `CREATE DATABASE`, `CREATE TABLE` with custom datatypes  
  - Support for defaults and auto-increment (`SERIAL`)  
  - Strong typing system: `INT`, `FLOAT`, `BOOLEAN`, `CHAR`, `VARCHAR`, `DATE`, `TIME`, `TIMESTAMP`  

- **Query Execution**  
  - `SELECT`, `INSERT`, `UPDATE`, `DELETE`  
  - `WHERE` clauses with:
    - Comparison operators: `=`, `!=`, `<`, `<=`, `>`, `>=`
    - Logical operators: `AND`, `OR`, `NOT`
    - Parentheses for grouping expressions
    - `IN`, `IS NULL`, `IS NOT NULL`, `BETWEEN`, `LIKE`, `ILIKE`
  - Aggregate functions: `COUNT`, `MAX`, `MIN`, `AVG`, `SUM`
  - `GROUP BY`, `HAVING`, `ORDER BY`  

- **Execution Engine**  
  - Custom lexer, parser, and AST (Abstract Syntax Tree)  
  - Condition evaluation with type-aware comparisons (`DATE`, `TIME`, `BOOLEAN`, etc.)  
  - Auto-increment columns (`SERIAL`) with sequence tracking  
  - Pretty-printed query results  

- **Persistence**  
  - Databases saved to disk (`.su` files) using [MessagePack](https://msgpack.org/)  
  - `.su_cache` file for tracking most recent database  
  - Auto-reconnect to last used database  

- **Interactive Shell**  
  - Multi-line query support  
  - `ls` → list tables with row/column counts  
  - `clear` → clear the screen  

---

## Why this project?

**PySQL** was built to explore database internals and give hands-on experience with:  

- How SQL is parsed into an AST  
- How query executors evaluate conditions, aggregations, and grouping  
- How databases manage schemas, defaults, and type validation  
- How persistence and caching can be added to an in-memory engine  

It’s not a production-ready database, but it provides a **realistic playground** for building and understanding database systems.

---

## Usage

1. **Run the interactive shell:**

```bash
python3 shell.py
```
2. **Execute queries:**

```Sql
CREATE TABLE users (
    id SERIAL,
    name VARCHAR DEFAULT = "Guest",
    age INT DEFAULT = 18,
    joined DATE DEFAULT = "2025-01-01"
);

INSERT INTO users (name, age) VALUES ("Hamza", 19);

SELECT name, age 
FROM users 
WHERE age BETWEEN 18 AND 30 
ORDER BY age DESC;
```

3. **Aggregate example:**
```sql
SELECT department, COUNT(*), AVG(salary)
FROM employees
GROUP BY department
HAVING AVG(salary) > 5000;
```
---

## Future Work

 
  - Support for `JOINs` (INNER, LEFT, RIGHT) 
  - Query planner and optimizer
  - Support for views and subqueries  
  - User-defined functions
  - Indexing structures (B-tree, Hash index)
  - Transaction support (BEGIN, COMMIT, ROLLBACK)


