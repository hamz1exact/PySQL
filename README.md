# su-sql

**su-sql** is a lightweight, in-memory SQL-like database engine implemented in Python. It supports basic SQL operations and provides a simple, interactive shell for querying and managing data. The project is designed for learning, experimentation, and practicing database internals, parsing, and query execution.

---

## Features

- **SELECT queries** with support for multiple columns  
- **WHERE clauses** with comparison operators (`=`, `!=`, `<`, `<=`, `>`, `>=`)  
- **Basic query parsing** using a custom lexer and parser  
- **In-memory database** represented as Python dictionaries  
- **Pretty-printed query results** for easy readability  
- **Interactive shell commands**:  
  - `ls` → list all tables and row counts  
- **Extensible architecture** for adding more SQL features like `INSERT`, `UPDATE`, `DELETE`, and logical operators (`AND`, `OR`)  

---

## Why this project?

**su-sql** is a learning-focused project to understand:

- How SQL queries are parsed into an AST (Abstract Syntax Tree)  
- How executors filter and project rows  
- How lexers and parsers work in Python  
- How to structure an in-memory database engine  

> ⚠️ It is not intended for production use, but it provides a hands-on foundation for anyone who wants to dive into database internals.

---

## Usage

1. **Run the interactive shell:**

```bash
python shell.py
```

2. **Execute queries:**

```sql
SELECT name, age FROM users WHERE age > 18;
```

3. **List all tables:**

``` bash
ls
```


## Future Work

- Nested and multiple logical operators (`AND`, `OR`) in `WHERE` clauses
- Advanced string handling in `WHERE` (pattern matching, LIKE, etc.)
- Additional SQL operations:
  - `UPDATE`
  - `DELETE`
- Database schema management:
  - `CREATE TABLE`
  - `DROP TABLE`
- Persistence to disk (custom `.su-sql` files or JSON format)
- Auto-increment improvements for primary keys
- Basic indexing for faster lookups and filtering
- More aggregate functions: `COUNT()`, `SUM()`, `AVG()`, `MIN()`, `MAX()`
- Enhanced error handling with detailed messages
- Optional integration with external storage (CSV import/export)