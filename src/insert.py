import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))
from exec.sql_helpers import *

def execute_insert_query(ast, database):
    table_name = ast.table
    
    if table_name not in database:
        raise ValueError(f"Table '{table_name}' does not exist")
    
    table_obj = database[table_name]
    table_rows = table_obj.rows
    table_schema = table_obj.schema
    table_default = table_obj.defaults
    table_auto = table_obj.auto
    table_constraints = getattr(table_obj, 'constraints', {})
    table_restrictions = table_obj.restrictions
    
    inserted_rows = []  # Track all inserted rows for RETURNING
    
    for object in ast.insertion_data:
        columns = object.columns
        values = object.values
        
        if not columns:
            columns = [col for col in table_schema.keys()]
        
        if len(values) != len(columns):
            raise ValueError(
                f"Number of values ({len(values)}) does not match number of columns ({len(columns)}). "
                f"Columns: {columns}, Values: {values}"
            )
    
        # Build new row
        new_row = {}
        temp_auto_values = {}
        for col_object, col_val in table_schema.items():
            if col_object in columns:
                idx = columns.index(col_object)
                val = values[idx]
                new_row[col_object] = table_schema[col_object](val)
            elif col_object in table_auto: 
                temp_auto_values[col_object] = table_auto[col_object].current
                new_row[col_object] = temp_auto_values[col_object]
            elif col_object in table_default:
                default_expr = table_default[col_object]
                if hasattr(default_expr, 'evaluate'):
                    raw_value = default_expr.evaluate()
                    new_row[col_object] = table_schema[col_object](raw_value)
                else:
                    new_row[col_object] = default_expr
            else:
                if col_object in table_constraints:
                    if table_constraints[col_object] == "NOT NULL" or table_constraints[col_object] == "PRIMARY KEY":
                        raise ValueError(f"Column <{col_object}> cannot be null; it uses a NOT NULL constraint.")
                else:
                    new_row[col_object] = None
            if col_object in table_restrictions:
                
                if not table_restrictions[col_object].evaluate(new_row, table_schema):
                    raise ValueError(f"new row for relation '{table_name}' violates check constraint of column <{col_object}>")
        
        violation = find_constraint_violation(table_obj, new_row)
        should_insert = handle_conflict_resolution(ast, violation, table_obj, new_row)
        
        for col, val in temp_auto_values.items():
            table_auto[col].current += 1
            
        if should_insert:
            table_rows.append(new_row)
            inserted_rows.append(new_row)  # Add to our tracking list
            print(f"Row successfully inserted into table '{table_name}'")
    
    return inserted_rows  # Return all inserted rows
