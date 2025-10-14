import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))
from exec.sql_helpers import *

def execute_delete_query(ast, database):
    if ast.table not in database:
        raise ValueError(f"Table '{ast.table}' does not exist")
    if ast.where == None:
        raise ValueError('Deleting all rows using DELETE statement is NOT allowed, use TRUNCATE TABLE <table_name> instead')
    table_obg = database[ast.table]
    table_rows = table_obg.rows
    table_schema = table_obg.schema
    n = 0
    deleted_rows = []
    for i in range(len(table_rows) - 1, -1, -1):  # start from last data row
        row = table_rows[i]
        if ast.where.evaluate(row, table_schema):
            deleted_rows.append(table_rows[i])
            del table_rows[i]
            n += 1
    print(f"{n} rows were deleted")
    return deleted_rows
    