import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))
from exec.sql_helpers import *

def create_temp_cte_table(ast, db_manager):
    cte_rows = ast.query.evaluate()
    cte_schema = generate_schema(cte_rows)
    new_table = Table(
        name=ast.table_name, 
        schema=cte_schema, 
        defaults={}, 
        auto={}, 
        constraints={}, 
        restrictions={}, 
        private_constraints={}, 
        constraints_ptr={}
    )
    for row in cte_rows:
        converted_rows = {}
        for col, val in row.items():
            if val is None:
                converted_rows[col] = None
            else:
                try:
                    sql_type_class = cte_schema[col]
                    converted_rows[col] = sql_type_class(val)
                except Exception as e:
                    print(f"Error converting value {val} ({type(val)}) to {sql_type_class.__name__} for column '{col}': {e}")
                    converted_rows[col] = VARCHAR(str(val))
        new_table.rows.append(converted_rows)
    db_manager.active_db[ast.cte_name] = new_table