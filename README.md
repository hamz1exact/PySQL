# PySQL

**PySQL** is a comprehensive SQL database engine in Python with advanced features like CTEs, materialized views, subqueries, and constraint management. Built for learning database internals and query execution.
---

## Table of Contents
- [Features](#features)
- [Why This Project?](#why-this-project)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Supported Features](#supported-features)
- [Interactive Shell Commands](#interactive-shell-commands)
- [Examples](#examples)
- [Project Structure](#project-structure)

---

## Features

### Core Database Operations
- **Schema Management**: CREATE/DROP DATABASE, TABLE, VIEW, MATERIALIZED VIEW
- **Data Manipulation**: INSERT, UPDATE, DELETE with RETURNING support
- **Query Operations**: SELECT with comprehensive WHERE clauses, subqueries, aggregations
- **Constraints**: PRIMARY KEY, UNIQUE, CHECK, NOT NULL with conflict resolution
- **Advanced Features**: CTEs, set operations (UNION/INTERSECT/EXCEPT), materialized views

### Data Types & Functions
- **Type System**: INT, FLOAT, BOOLEAN, VARCHAR, CHAR, TEXT, DATE, TIME, TIMESTAMP, SERIAL
- **Aggregate Functions**: COUNT, MAX, MIN, AVG, SUM with GROUP BY/HAVING
- **String Functions**: UPPER, LOWER, LENGTH, SUBSTRING, CONCAT, REPLACE
- **Math Functions**: ROUND, CEIL, FLOOR, ABS
- **Date Functions**: EXTRACT, DATEDIFF, CURRENT_DATE, NOW
- **Conditional Logic**: CASE WHEN, COALESCE, NULLIF, CAST

### Query Features
- **Filtering**: WHERE with AND/OR/NOT, IN, BETWEEN, LIKE/ILIKE, IS NULL
- **Ordering & Limiting**: ORDER BY, DISTINCT, LIMIT/OFFSET
- **Subqueries**: Support in WHERE and FROM clauses
- **Advanced Clauses**: GROUP BY, HAVING with complex expressions
- **Table Operations**: ALTER TABLE for columns and constraints

---

## Why This Project?

PySQL was built to explore database internals and provide hands-on experience with:

- SQL parsing and Abstract Syntax Tree (AST) construction
- Query execution engines and condition evaluation
- Schema management and type validation systems
- Data persistence and caching mechanisms
- Database constraint enforcement and conflict resolution

It serves as a realistic playground for understanding how production databases work internally, without the complexity of a full RDBMS.

---

## Installation

### Prerequisites
- Python 3.8 or higher
- pip package manager

### Setup

1. **Clone the repository:**
```bash
git clone https://github.com/hamz1exact/PySQL.git
cd pysql
```

2. **Create and activate a virtual environment:**
```bash
# Create virtual environment
python3 -m venv venv

# Activate on Linux/Mac
source venv/bin/activate

# Activate on Windows
venv\Scripts\activate
```

3. **Install dependencies:**
```bash
pip install -r requirements.txt
```

---

## Quick Start

### Launch the Interactive Shell
```bash
python3 shell.py
```

### Basic Usage Example
```sql
-- Create a database and table
CREATE DATABASE my_app;
USE my_app;

CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    username VARCHAR UNIQUE,
    email VARCHAR CHECK (LENGTH(email) > 5),
    age INT CHECK (age >= 0),
    created_date DATE DEFAULT CURRENT_DATE,
    is_active BOOLEAN DEFAULT true
);

-- Insert some data
INSERT INTO users (username, email, age) 
VALUES 
    ('alice123', 'alice@example.com', 28),
    ('bob_dev', 'bob@example.com', 32),
    ('charlie', 'charlie@example.com', 25);

-- Query with aggregation
SELECT 
    CASE 
        WHEN age < 30 THEN 'Young'
        ELSE 'Experienced'
    END as age_group,
    COUNT(*) as user_count,
    AVG(age) as avg_age
FROM users 
WHERE is_active = true
GROUP BY age_group
ORDER BY avg_age DESC;
```

---

## Supported Features

<div align="center">

| **Category**              | **Feature**                                  | **Status** |
|---------------------------|----------------------------------------------|------------|
| **Database Management**   | CREATE DATABASE                              | ✅         |
|                           | CREATE TABLE                                 | ✅         |
|                           | CREATE VIEW                                  | ✅         |
|                           | CREATE MATERIALIZED VIEW                     | ✅         |
|                           | DROP TABLE/DATABASE/VIEW                     | ✅         |
|                           | PRIMARY KEYS                                 | ✅         |
|                           | UNIQUE Constraint                            | ✅         |
|                           | CHECK Constraint                             | ✅         |
|                           | ALTER TABLES/COLUMNS/CONSTRAINTS             | ✅         |
|                           | ON CONFLICT DO NOTHING/UPDATE                | ✅         |
|                           | CTE (Common Table Expressions)               | ✅         |
| **Data Manipulation**     | INSERT INTO                                  | ✅         |
|                           | UPDATE (Advanced)                            | ✅         |
|                           | DELETE                                       | ✅         |
|                           | RETURNING                                    | ✅         |
| **Querying**              | SELECT FROM                                  | ✅         |
|                           | WHERE (Complex conditions)                   | ✅         |
|                           | ORDER BY / DISTINCT                          | ✅         |
|                           | LIMIT / OFFSET                               | ✅         |
|                           | GROUP BY / HAVING                            | ✅         |
|                           | SUBQUERIES (WHERE/FROM)                      | ✅         |
|                           | IN / BETWEEN / LIKE / IS NULL                | ✅         |
| **Functions**             | Aggregates (COUNT/SUM/AVG/MIN/MAX)          | ✅         |
|                           | String Functions                             | ✅         |
|                           | Math Functions                               | ✅         |
|                           | Date/Time Functions                          | ✅         |
|                           | CASE WHEN / CAST / COALESCE                  | ✅         |
| **Set Operations**        | UNION / UNION ALL                            | ✅         |
|                           | INTERSECT / EXCEPT                           | ✅         |

</div>

---

## Interactive Shell Commands

Beyond SQL queries, the shell supports special commands:

- `\l` - List all tables with row and column counts
- `\dt` - List all databases
- `\clear` - Clear the screen
- `\export csv filename.csv` - Export query results to CSV
- `\import filename.sql` - Import and execute SQL from file
- `\quit` or `\exit` - Exit the shell

---

## Examples
****
### Advanced Query with Subqueries
```sql
-- Find users who registered above average age in their country
SELECT username, email, age, country
FROM users u1
WHERE u1.age > (
    SELECT AVG(u2.age)
    FROM users u2 
    WHERE u2.country = u1.country
)
ORDER BY country, age DESC;
```

### Using CTEs and Window-like Functions
```sql
WITH high_value_orders AS (
    SELECT customer_id, order_total, order_date
    FROM orders 
    WHERE order_total > 1000
),
customer_stats AS (
    SELECT 
        customer_id,
        COUNT(*) as order_count,
        AVG(order_total) as avg_order
    FROM high_value_orders
    GROUP BY customer_id
)
```

### Materialized Views and Refresh
```sql
-- Create materialized view for expensive query
CREATE MATERIALIZED VIEW sales_summary AS
SELECT 
    DATE(sale_date) as sale_day,
    COUNT(*) as total_sales,
    SUM(amount) as revenue,
    AVG(amount) as avg_sale
FROM sales 
WHERE sale_date >= '2024-01-01'
GROUP BY DATE(sale_date);

-- Query the materialized view (fast)
SELECT * FROM sales_summary WHERE revenue > 10000;

-- Refresh when underlying data changes
REFRESH MATERIALIZED VIEW sales_summary;
```

---

## Project Structure

```
pysql/
├── shell.py              # Interactive shell interface
├── engine.py             # SQL parser and lexer
├── sql_ast.py           # Abstract Syntax Tree definitions
├── executor.py          # Query execution engine
├── database_manager.py  # Database storage and management
├── datatypes.py         # SQL data type implementations
├── utilities.py         # Helper functions and utilities
├── errors.py           # Custom exception classes
├── requirements.txt     # Python dependencies
└── README.md           # This file
```

---

## Technical Details

### Architecture
- **Lexer**: Tokenizes SQL strings into meaningful symbols
- **Parser**: Builds Abstract Syntax Trees from tokens
- **Executor**: Traverses AST and executes operations on data
- **Storage**: Uses MessagePack for efficient serialization to disk

### Persistence
- Databases stored as `.su` files in user home directory
- Automatic caching of last used database
- Type-safe serialization/deserialization of all SQL types

### Performance Considerations
- In-memory operation for fast queries
- Optimized aggregate function implementations

---

## Acknowledgments

Built for educational purposes to demonstrate database internals and SQL parsing techniques. Inspired by production databases like PostgreSQL and SQLite, but designed for learning rather than production use.
