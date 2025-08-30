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
        
from sql2 import Lexer, Parser  # import the formatter

while True:
    try:
        query = input("> ")
        lexer = Lexer(query)
        parser = Parser(lexer.tokens)
        rows = parser.parse_select_statement()
        print_table(rows)  # pretty print
    except KeyboardInterrupt:
        print("\nExiting...")
        exit()
    except Exception as e:
        print("Error:", e)