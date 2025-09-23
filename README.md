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
  - `\ls` → list tables with row/column counts  
  - `\dt` → list active databases  
  - `\clear` → clear the screen  
  -  `\export csv csv_file.csv` → to export queries as csv  
  - `\import sqlfile.sql` → to import ready sql insert queries
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


1. **Set up a Python environment:**

```bash
# Create a virtual environment named 'venv'
python3 -m venv venv

# Activate the environment
# On Linux/Mac:
source venv/bin/activate
# On Windows:
venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

2. **Run the interactive shell:**:
```python
python3 shell.py
```


3. **Execute queries:**

```Sql
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    name VARCHAR DEFAULT = "Guest",
    age INT DEFAULT = 18,
    joined DATE DEFAULT CURRENT_DATE
);

INSERT INTO users (name, age) VALUES ("Hamza", 19);

SELECT name, age 
FROM users 
WHERE age BETWEEN 18 AND 30 
ORDER BY age DESC;
```

4. **Aggregate example:**
```sql
SELECT department, COUNT(*), AVG(salary)
FROM employees
GROUP BY department
HAVING AVG(salary) > 5000;
```
5. **Much More examples:**

- **You can import any SQL queries you have, while respecting my custom datatypes. For example, in standard SQL you write VARCHAR(length) (e.g., VARCHAR(255)), but in my engine there is no length limit, so you should only use VARCHAR.**
- **Ready tables folder has more examples.**

## Supported Features in su-sql



<div align="center">

| **Category**              | **Feature**                                  | **Status** |
|---------------------------|----------------------------------------------|------------|
| **Database Management**   | CREATE DATABASE                              | ✅         |
|                           | CREATE TABLE                                 | ✅         |
|                           | CREATE VIEW                                  | ✅         |
|                           | CREATE MATERIALIZED VIEW                     | ✅         |
|                           | DROP TABLE                                   | ✅
|                           | DROP DATABASE                                | ✅
|                           | DROP VIEW                                    | ✅
|                           | DROP MATERIALIZED VIEW                       | ✅
|                           | PRIMARY KEYS                                 | ✅         |
|                           | UNIQUE Constraint                            | ✅         |
|                           | CHECK Constraint                             | ✅         |
|                           | ON CONFLICT DO NOTHING                       | ✅         |
|                           | UPSERT                                       | ✅         |
|                           | CTE (Common Table Expressions)               | ✅         |
| **Data Manipulation**     | INSERT INTO                                  | ✅         |
|                           | UPDATE                                       | ✅         |
|                           | ADVANCED UPDATE                              | ✅         |
|                           | DELETE                                       | ✅         |
|                           | RETURNING                                    | ✅         |
| **Querying**              | SELECT FROM (no joins yet)                   | ✅         |
|                           | ORDER BY                                     | ✅         |
|                           | DISTINCT                                     | ✅         |
|                           | WHERE                                        | ✅         |
|                           | AND / OR / NOT                               | ✅         |
|                           | LIMIT / OFFSET / FETCH                       | ✅         |
|                           | IN                                           | ✅         |
|                           | BETWEEN                                      | ✅         |
|                           | LIKE and ILIKE                               | ✅         |
|                           | IS NULL                                      | ✅         |
|                           | GROUP BY                                     | ✅         |
|                           | HAVING                                       | ✅         |
|                           | ALIASES in tables and columns                | ✅         |
|                           | SUBQUERIES in WHERE                          | ✅         |
|                           | SUBQUERIES in FROM                           | ✅         |
| **Functions & Expressions** | MIN / MAX / SUM / AVG                       | ✅         |
|                           | CASE WHEN                                    | ✅         |
|                           | ADVANCED CASE WHEN                           | ✅         |
|                           | CASE WHEN + SUM                              | ✅         |
|                           | CAST                                         | ✅         |
|                           | COALESCE                                     | ✅         |
|                           | NULLIF                                       | ✅         |
|                           | String formatting functions (various)        | ✅         |
| **Date & Time**           | Timestamps and Dates                         | ✅         |
|                           | Adding and Subtracting intervals             | ✅         |
|                           | Extracting fields from timestamps            | ✅         |
| **Set Operations**        | UNION / UNION ALL                            | ✅         |
|                           | INTERSECT                                    | ✅         |
|                           | EXCEPT                                       | ✅         |

</div>


---

## Future Work
  - ALTER DATABASES/TABLES/VIEWS
  - Query planner and optimizer
  - Advanced Update Using SELECT queries (Commercial DB level)
  - User-defined functions
  - Indexing structures (B-tree, Hash index)
  - Triggers
  - User Management
  - Managing Tables
  - Joins
  - Swapping
  - Window Functions (WF)
  - Transaction support (BEGIN, COMMIT, ROLLBACK)
  - Executin Plan
  - Documentation
  


