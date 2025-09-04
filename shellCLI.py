import sys
from engine import db_manager



def print_table(rows):
    if not rows:
        print("Empty result")
        return

    # Get columns from the first row
    columns = list(rows[0].keys())

    # Calculate column widths
    col_widths = {col: len(col) for col in columns}
    for row in rows:
        for col in columns:
            col_widths[col] = max(col_widths[col], len(str(row[col])))

    # Print header
    header = " | ".join(col.ljust(col_widths[col]) for col in columns)
    print(header)
    print("-" * len(header))

    # Print rows
    for row in rows:
        line = " | ".join(str(row[col]).ljust(col_widths[col]) for col in columns)
        print(line)
        
        
def show_tables(database):
    # Collect data
    rows = [(name, len(rows)) for name, rows in database.items()]

    # Determine column widths
    col1_width = max(len("Table Name"), max(len(name) for name, _ in rows))
    col2_width = max(len("Total Rows"), max(len(str(count)) for _, count in rows))

    # Print header
    print(f"+{'-'*(col1_width+2)}+{'-'*(col2_width+2)}+")
    print(f"| {'Table Name'.ljust(col1_width)} | {'Total Rows'.ljust(col2_width)} |")
    print(f"+{'-'*(col1_width+2)}+{'-'*(col2_width+2)}+")

    # Print each row
    for name, count in rows:
        print(f"| {name.ljust(col1_width)} | {str(count).rjust(col2_width)} |")

    # Footer line
    print(f"+{'-'*(col1_width+2)}+{'-'*(col2_width+2)}+")
    
    
    
from engine import Lexer, Parser  # import the formatter
from executor import execute

while True:
    database = db_manager.active_db
    try:
        # ---------------- Read multi-line query until semicolon ----------------
        query_lines = []
        while True:
            line = input("> ")
            query_lines.append(line)
            if "clear" in line or "ls" in line:
                break
            if ";" in line:
                break
        query = " ".join(query_lines).strip()

        # ---------------- Handle shell commands ----------------
        if query.lower() == "clear":
            import os
            os.system("cls" if os.name == "nt" else "clear")
            continue

        if query.lower() == "ls":
            parser = Parser([])
            show_tables(database)
            continue

        # ---------------- Lexer & Parser ----------------
        lexer = Lexer(query)
        parser = Parser(lexer.tokens)

        if not lexer.tokens:
            continue  # skip empty input

        token_type = lexer.tokens[0][0]
        next_token_type = lexer.tokens[1][0]
        if token_type == "SELECT":
            ast = parser.parse_select_statement()
            rows = execute(ast, database)
            print_table(rows)
        elif token_type == "INSERT":
            ast = parser.parse_insert_statement()
            execute(ast, database)
            db_manager.save_database_file()
        elif token_type == "UPDATE":
            ast = parser.parse_update_statement()
            execute(ast, database)
            db_manager.save_database_file()
        elif token_type == "DELETE":
            ast = parser.parse_delete_statement()
            execute(ast, database)
        elif token_type == "CREATE":
            if next_token_type == "DATABASE":
                ast = parser.parse_create_database()
            elif next_token_type == "TABLE":
                ast = parser.parse_create_table()
        elif token_type == "USE":
             ast = parser.parse_use_statement()
        else:
            raise ValueError(f"Invalid Keyword '{lexer.tokens[0][1]}'")

    except KeyboardInterrupt:
        exit()
    # except KeyError as k:
    #     print(f"Row / column {k} not found")
    # except Exception as e:
    #     print("Error:", e)
