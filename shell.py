import sys
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

while True:
    try:
        query = input("\n> ")
        print()
        if query.lower() == "clear":
            import os
            if os.name == 'nt':
                os.system('cls')
            else:
                os.system('clear')
            continue
        if query.lower() == "ls":
            parser = Parser([])
            show_tables(parser.database)
            continue
        lexer = Lexer(query)
        parser = Parser(lexer.tokens)
        rows = parser.parse_select_statement()
        
        print_table(rows)  # pretty print
        
    except KeyboardInterrupt:
        exit()
    except KeyError as k:
        print(f"Row / column {k} Not Found")
    except Exception as e:
        print("Error:", e)