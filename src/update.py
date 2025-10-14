import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))
from exec.sql_helpers import *

def execute_update_query(ast, database):
    table_name = ast.table
    if table_name not in database:
        raise ValueError(f"Table '{table_name}' does not exist")
    table_obj = database[table_name]
    table_rows = table_obj.rows
    table_schema = table_obj.schema
    cnt = 0
    for column in ast.columns.keys():
        if column not in table_schema:
            raise ValueError(f"Column '{column}' does not exist")

    inserted_rows = []
    for row in table_rows:
        if ast.where is None or ast.where.evaluate(row, table_schema):
            for col, expression in ast.columns.items():
                row[col] = expression.evaluate(row ,table_schema)
                inserted_rows.append(row)
                cnt += 1
            
    print(f"{cnt} row(s) updated in '{table_name}'")
    return inserted_rows