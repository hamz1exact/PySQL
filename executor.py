from sql_ast import Condition, LogicalCondition, SelectStatement, InsertStatement, UpdateStatement, DeleteStatement
from checker import CheckDate, CheckTime, CheckDataType
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
    if requested_columns == ['*']:
        columns_to_return = table[0].keys() if table else []
    else:
        for col in requested_columns:
            if table and col not in table[0]:
                raise ValueError(f"Column '{col}' does not exist in table '{table_name}'")
        columns_to_return = requested_columns
    result = []
    for row in table:
        if ast.where is None or condition_evaluation(ast.where, row):
            selected_row = {col: row[col] for col in columns_to_return if row[col] is not None}
            result.append(selected_row)
    return result



def condition_evaluation(where, row):
    
    if isinstance(where, Condition):
        if type(row[where.column]) == str:
            left = row[where.column].lower()
        else:
            left = row[where.column]
        right = where.value
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
        left_result = condition_evaluation(where.left, row)
        right_result = condition_evaluation(where.right, row)
        
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
        schema_type = CheckDataType(schema_val)
        if schema_col in columns:
            idx = columns.index(schema_col)
            val = values[idx]
            if type(val) != schema_type:
                raise ValueError(f"Expected {schema_type} DataType, But {type(val)} Were Given")
            else:
                if schema_col in table_auto:
                    table_auto[schema_col]  = val
                new_row[schema_col] = val
        else:
            if schema_col in table_auto:
                table_auto[schema_col] += 1
                new_row[schema_col] = table_auto.get(schema_col)
            elif schema_col in table_default:
                new_row[schema_col] = table_default[schema_col]
            else:
                new_row[schema_col] = None
                
    table_rows.append(new_row)
    print(f"Row successfully inserted into table '{table_name}'")
    
        

def execute_update_query(ast, database):
    table_name = ast.table
    if table_name not in database:
        raise ValueError(f"Table '{table_name}' does not exist")
    table = database[table_name][2:]
    DT = database[table_name][0]
    cnt = 0
    for row in table:
        for col in row:
            if col in ast.columns and (ast.where is None or condition_evaluation(ast.where, row)):
                if DT[col] == int:
                    try:
                        row[col] = int(ast.columns[col])
                        cnt += 1
                        break
                    except Exception as e:
                        raise ValueError(f"Error converting value for column '{col}': {e}")
                elif DT[col] == bool:
                    try:
                        if ast.columns[col].upper() == "TRUE":
                            row[col] = True
                            cnt += 1
                            break
                        if ast.columns[col].upper() == "FALSE":
                            row[col] = False
                            cnt += 1
                            break
                    except Exception as e:
                        raise ValueError(f"Error converting value for column '{col}': {e}")
                else:
                    if ast.columns[col].isdigit():
                        raise ValueError(
                                f"Invalid value for column '{col}': expected a string (non-numeric), got digits only -> '{ast.columns[col]}'")
                row[col] = str(ast.columns[col])
                cnt += 1
    print(f"{cnt} row(s) updated in '{table_name}'")
    
    
def execute_delete_query(ast, database):
    if ast.table not in database:
        raise ValueError(f"Table '{ast.table}' does not exist")
    if ast.where == None:
        n = len(database[ast.table])
        del database[ast.table][:]
        print(f"{n} rows Deleted")
        return
    table = database[ast.table]
    n = 0
    for i in range(len(table) - 1, 1, -1):  # start from last data row
        row = table[i]
        if condition_evaluation(ast.where, row):
            del table[i]
            n += 1
    print(f"{n} rows deleted")
    
    
