from sql_ast import *
from database_manager import Table
from checker import *
from datatypes import SQLType
def execute(ast, database):
    if isinstance(ast, SelectStatement):
        return execute_select_query(ast, database)
    elif isinstance(ast, InsertStatement):
        execute_insert_query(ast, database)
    elif isinstance(ast, UpdateStatement):
        execute_update_query(ast, database)
    elif isinstance(ast, DeleteStatement):
        execute_delete_query(ast, database)
    elif isinstance(ast, CreateDatabseStatement):
        execute_create_database_statement(ast, database)
    elif isinstance(ast, CreateTableStatement):
        execute_create_table_statement(ast, database)
    elif isinstance(ast, UseStatement):
        execute_use_statement(ast, database)
    

def execute_select_query(ast, database):
    table_name = ast.table
    
    if table_name not in database:
        raise ValueError(f"Table '{table_name}' does not exist")
    
    table = database[table_name].rows
    table_schema = database[table_name].schema
    # Filter where rows
    filtered_rows = []
    for row in table:
        if ast.where is None or condition_evaluation(ast.where, row, table_schema):
            filtered_rows.append(row)
    if ast.columns == ["*"]:
        ast.columns = table[0].keys()
    result = []
    if any(isinstance(col, FunctionCall) for col in ast.columns):
        result_row = {}
        columns_to_return = table[0].keys() if table else []
        for col in ast.columns:
            if isinstance(col, FunctionCall):
                result_row[col.alias] = execute_function(col, filtered_rows)
            result = [result_row]   
    else:
        for row in filtered_rows:
            selected_row = {col: row.get(col) for col in ast.columns}
            result.append(serialize_row(selected_row))
    return result

def condition_evaluation(where, row, table_schema):
    if isinstance(where, Condition):
        col = row[where.column]
        val = table_schema[where.column](where.value)
        op  = where.operator
        
        if op == "=": return col == val
        if op == "!=": return col != val
        if op == "<": return col < val
        if op == "<=": return col <= val
        if op == ">": return col > val
        if op == ">=": return col >= val
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
    for col_name, col_val in table_schema.items():
        if col_name in columns:
            idx = columns.index(col_name)
            val = values[idx]
            new_row[col_name] = table_schema[col_name](val)
        elif col_name in table_auto:
            new_row[col_name] = table_auto[col_name].next()
        elif col_name in table_default:
            new_row[col_name] = table_default[col_name]
        else:
            new_row[col_name] = None      
    table_rows.append(new_row)
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

    # Loop through rows
    for row in table_rows:
        if ast.where is None or condition_evaluation(ast.where, row, table_schema):
            for col, new_val in ast.columns.items():
                # Wrap raw Python value in correct SQLType
                row[col] = table_schema[col](new_val)
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
    table_obg = database[ast.table]
    table_rows = table_obg.rows
    table_schema = table_obg.schema
    n = 0
    for i in range(len(table_rows) - 1, -1, -1):  # start from last data row
        row = table_rows[i]
        if condition_evaluation(ast.where, row, table_schema):
            del table_rows[i]
            n += 1
    print(f"{n} rows deleted")
    
    
def serialize_row(row):
    """Convert SQLType objects to raw Python values for display or SELECT output."""
    return {col: (val.value if isinstance(val, SQLType) else val) for col, val in row.items()}


def execute_create_database_statement(ast, database):
        database.create_database(ast.database_name)
        database.use_database(ast.database_name)
        
def execute_create_table_statement(ast, database):
        table = Table(ast.table_name, ast.schema, ast.defaults, ast.auto)
        database.active_db[ast.table_name] = table
        database.save_database_file()
        
def execute_use_statement(ast, database):
        database.use_database(ast.database_name)
        database.save_database_file()


def execute_function(function, rows):
    func_name = function.function_name
    arguments = function.arg
    
    if func_name == "COUNT":
        if arguments == "*":
            return len(rows)
        else:
            return len([row for row in rows if (row.get(arguments).value if isinstance(row.get(arguments), SQLType) else row.get(arguments)) is not None])
        
            
        
        
    
    