from sql_ast import Condition, LogicalCondition, SelectStatement, InsertStatement, UpdateStatement, DeleteStatement
from checker import *
def execute(ast, database):
    if isinstance(ast, SelectStatement):
        return execute_select_query(ast, database)
    elif isinstance(ast, InsertStatement):
        execute_insert_query(ast, database)
    elif isinstance(ast, UpdateStatement):
        execute_update_query(ast, database)
    elif isinstance(ast, DeleteStatement):
        execute_delete_query(ast, database)
        
def execute_select_query(ast, database):
    table_name = ast.table
    if table_name not in database:
        raise ValueError(f"Table '{table_name}' does not exist")
    table = database[table_name].rows
    requested_columns = ast.columns
    table_schema = database[table_name].schema
    if requested_columns == ['*']:
        columns_to_return = table[0].keys() if table else []
    else:
        for col in requested_columns:
            if table and col not in table[0]:
                raise ValueError(f"Column '{col}' does not exist in table '{table_name}'")
        columns_to_return = requested_columns
    result = []
    for row in table:
        if ast.where is None or condition_evaluation(ast.where, row, table_schema):
            selected_row = {col: row.get(col) for col in columns_to_return}
            result.append(selected_row)
    return result



def condition_evaluation(where, row, table_schema):
    if isinstance(where, Condition):
        if where.column not in table_schema:
            raise ValueError(f"Where Column -> '{where.column}' does not exist")
        col_type = CheckDataType(table_schema[str(where.column)])
        if col_type in (float, int) and type(where.value) in (float, int):
            pass
        elif col_type != type(where.value):
            raise ValueError (f"Given Datatype {type(where.value)} does not match the default datatype of column {where.column} -> {col_type}\nPlease Write --help <data_type> (i.e --help {table_schema[str(where.column)].capitalize()}) for more information\n")
        

        if type(row[where.column]) == str:
            left = row[where.column].lower()
        else:
            left = row[where.column]
        
        right = str(where.value).lower() if type(where.value) == str else where.value
        op  = where.operator
        if op == "=": return left == right
        if op == "!=": return left != right
        if op == "<": return left < right
        if op == "<=": return left <= right
        if op == ">": return left > right
        if op == ">=": return left >= right
        raise ValueError(f"Unknown operator {op}")

    elif isinstance(where, LogicalCondition):

        MainOperator = where.MainOperator.upper()
        left_result = condition_evaluation(where.left, row, table_schema)
        right_result = condition_evaluation(where.right, row, table_schema)
        
        if MainOperator == "AND":
            return left_result and right_result
        elif MainOperator == "OR":
            return left_result or right_result
        else:
            raise ValueError(f"Unknown logical operator: {MainOperator}")

def execute_insert_query(ast, database):
    table_name = ast.table
    columns = ast.columns
    values = ast.values
    if table_name not in database:
        raise ValueError(f"Table '{table_name}' does not exist")
    table_obj = database[table_name]
    table_rows = table_obj.rows
    table_schema = table_obj.schema
    table_default = table_obj.defaults
    table_auto = table_obj.auto
    
    if len(values) != len(columns):
        raise ValueError(
            f"Number of values ({len(values)}) does not match number of columns ({len(columns)}). "
            f"Columns: {columns}, Values: {values}"
        )
    new_row = {}
    for schema_col, schema_val in table_schema.items():
        if schema_col in columns:
            idx = columns.index(schema_col)
            val = values[idx]
            eval_dp = data_validator(schema_col, schema_val, val)
            if eval_dp:
                val = getSchemaDataType(schema_val)(val)
                if schema_col in table_auto:
                    table_auto[schema_col]  = val
                else:
                    new_row[schema_col] = val
            else:
                print(eval_dp)
        else:
            if schema_col in table_auto:
                table_auto[schema_col] += 1
                new_row[schema_col] = table_auto.get(schema_col)
            elif schema_col in table_default:
                new_row[schema_col] = table_default[schema_col]
            else:
                new_row[schema_col] = None
                
    table_rows.append(new_row)
    print(new_row)
    print(f"Row successfully inserted into table '{table_name}'")
    
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
    
    for row in table_rows:
        if ast.where is None or condition_evaluation(ast.where, row, table_schema):
            for col, new_val in ast.columns.items():
                schema_dp = table_schema[col]              
                if col not in table_schema:
                    raise ValueError(f"Column '{col}' does not exist in table '{table_name}'")
                DVT = data_validator(col, schema_dp, new_val)
                if DVT == True:
                    new_val = getSchemaDataType(schema_dp)(new_val)
                    row[col] = new_val
                    cnt += 1
                else:
                    print(DVT)
    print(f"{cnt} row(s) updated in '{table_name}'")
    
def execute_delete_query(ast, database):
    if ast.table not in database:
        raise ValueError(f"Table '{ast.table}' does not exist")
    if ast.where == None:
        n = len(database[ast.table])
        del database[ast.table][:]
        print(f"{n} rows Deleted")
        return
    table_obg = database[ast.table]
    table_rows = table_obg.rows
    table_schema = table_obg.schema
    n = 0
    for i in range(len(table_rows) - 1, 1, -1):  # start from last data row
        row = table_rows[i]
        if condition_evaluation(ast.where, row, table_schema):
            del table_rows[i]
            n += 1
    print(f"{n} rows deleted")
    
    
