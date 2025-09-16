import traceback
from engine import Lexer, Parser
from executor import execute
from engine import db_manager

def run_queries_from_file(file_path, log_file="failed_queries.log"):
    # Read entire file
    with open(file_path, "r") as f:
        raw = f.read()

    # Split queries by semicolon (multi-line queries preserved)
    queries = [q.strip() + ";" for q in raw.split(";") if q.strip()]
    total = len(queries)

    # Reset log file
    with open(log_file, "w") as log:
        log.write("Failed Queries Log\n\n")

    for i, query in enumerate(queries, start=1):
        try:
            lexer = Lexer(query)
            parser = Parser(lexer.tokens)
            ast = None

            # Detect query type
            if lexer.tokens[0][0] == "SELECT":
                ast = parser.parse_select_statement()
            elif lexer.tokens[0][0] == "INSERT":
                ast = parser.parse_insert_statement()
            elif lexer.tokens[0][0] == "UPDATE":
                ast = parser.parse_update_statement()
            elif lexer.tokens[0][0] == "DELETE":
                ast = parser.parse_delete_statement()
            elif lexer.tokens[0][0] == "CREATE":
                nxt = lexer.tokens[1][0] if len(lexer.tokens) > 1 else None
                if nxt == "DATABASE":
                    ast = parser.parse_create_database()
                elif nxt == "TABLE":
                    ast = parser.parse_create_table()
            elif lexer.tokens[0][0] == "USE":
                ast = parser.parse_use_statement()

            # Execute
            if ast:
                execute(ast, db_manager.active_db)

            print(f"test {i} / {total} success")

        except Exception as e:
            print(f"test {i} / {total} failed")
            with open(log_file, "a") as log:
                log.write(f"--- Test {i} Failed ---\n")
                log.write(f"Query:\n{query}\n")
                log.write(f"Error: {str(e)}\n")
                log.write(traceback.format_exc() + "\n\n")

if __name__ == "__main__":
    run_queries_from_file("queries.txt")