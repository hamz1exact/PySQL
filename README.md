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
-	Support for * wildcard in SELECT with WHERE
-	Logical operators (AND, OR) in WHERE clauses
-	String comparisons in WHERE
-	INSERT, UPDATE, DELETE operations
-	Persistence to disk (custom .su-sql files)
-	Indexing and performance optimizations