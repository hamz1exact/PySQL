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
cd PySQL
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

**Easy way (recommended):**
```bash
python3 main.py
```

**Alternative:**
```bash
python3 cli/shell.py
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

### Database Operations
- `\l` - List all tables with row and column counts
- `\dt` - List all databases
- `\c <database>` - Connect to a database
- `\use <database>` - Same as \c

### Display Options
- `\normal` - Set normal table display mode
- `\auto` - Set auto table display mode
- `\wide vertical` - Show last result in vertical format
- `\cols` - Show specific columns from last result
- `\csv` - Show last result in CSV format
- `\schema` - Show column names and types

### Configuration
- `\timing` - Toggle query timing on/off
- `\echo` - Toggle query echo on/off
- `\set <option> <value>` - Set configuration option
- `\show [option]` - Show configuration

### Data Operations
- `\export <format> <file>` - Export last SELECT result (csv, json, sql, xlsx)
- `\import <file>` - Import and execute SQL from file

### Utility
- `\clear` - Clear the screen
- `\history` - Show command history
- `\status` - Show session status
- `\version` - Show version information
- `\help` - Show all commands
- `\quit` or `\exit` - Exit the shell

---

## Examples

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

### Using CTEs (Common Table Expressions)
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
SELECT * FROM customer_stats
WHERE order_count > 5;
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

### Set Operations
```sql
-- UNION: Combine results from two queries
SELECT name, email FROM customers
UNION
SELECT name, email FROM suppliers;

-- INTERSECT: Find common records
SELECT product_id FROM orders_2023
INTERSECT
SELECT product_id FROM orders_2024;

-- EXCEPT: Find records in first query but not in second
SELECT customer_id FROM all_customers
EXCEPT
SELECT customer_id FROM inactive_customers;
```

---

## Project Structure

```
pysql/
├── cli/                    # Interactive shell interface
│   └── shell.py           # Enhanced shell with autocomplete
├── engine/                 # SQL parsing engine
│   ├── lexer.py           # Tokenization
│   ├── parser.py          # SQL parser
│   └── sql_ast.py         # Abstract Syntax Tree definitions
├── exec/                   # Query execution
│   ├── exec.py            # Main execution coordinator
│   └── helpers.py         # Execution helper functions
├── src/                    # Core SQL operations
│   ├── select.py          # SELECT query implementation
│   ├── insert.py          # INSERT query implementation
│   ├── update.py          # UPDATE query implementation
│   ├── delete.py          # DELETE query implementation
│   ├── create.py          # CREATE operations
│   ├── drop.py            # DROP operations
│   └── constants.py       # SQL constants and keywords
├── sql_types/              # SQL data types
│   └── sql_types.py       # Type system implementation
├── storage/                # Data persistence
│   └── classes.py  
    └── database.py 
    └── table.py 
    └── serialize.py 
    └── deserialize.py 
    └── reference.py 
├── queries/                # Example SQL queries
├── main.py                 # Main entry point
├── errors.py               # Custom exception classes
├── utilities.py            # Helper functions
├── requirements.txt        # Python dependencies
└── README.md              # This file
```

---


## Technical Details

### Architecture
- **Lexer**: Tokenizes SQL strings into meaningful symbols using regex patterns
- **Parser**: Builds Abstract Syntax Trees from tokens using recursive descent parsing
- **Executor**: Traverses AST and executes operations on in-memory data structures
- **Storage**: Uses MessagePack for efficient binary serialization to disk

### Persistence
- Databases stored as `.su` files in user home directory (`~/.pysql/`)
- Automatic caching of last used database
- Type-safe serialization/deserialization of all SQL types
- Supports concurrent reads (single writer)

### Performance Considerations
- In-memory operation for fast queries
- Optimized aggregate function implementations
- Efficient condition evaluation using Python's native operators
- Materialized views for expensive query caching

### Supported SQL Syntax
```sql
-- Data Definition Language (DDL)
CREATE DATABASE name;
CREATE TABLE name (columns...);
ALTER TABLE name ADD COLUMN/CONSTRAINT;
DROP TABLE/DATABASE/VIEW name;

-- Data Manipulation Language (DML)
INSERT INTO table VALUES (...) RETURNING *;
UPDATE table SET col=val WHERE condition RETURNING *;
DELETE FROM table WHERE condition RETURNING *;

-- Data Query Language (DQL)
SELECT columns FROM table
WHERE condition
GROUP BY columns
HAVING condition
ORDER BY columns
LIMIT n OFFSET m;

-- Advanced Features
WITH cte AS (SELECT ...) SELECT ... FROM cte;
CREATE MATERIALIZED VIEW name AS SELECT ...;
REFRESH MATERIALIZED VIEW name;
```

---

## Acknowledgments

Built for educational purposes to demonstrate database internals and SQL parsing techniques. Inspired by production databases like PostgreSQL and SQLite.