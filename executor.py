from sql_ast import *
from database_manager import Table
from checker import *
from datatypes import *
from errors import *
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
    else:
        for col in ast.columns:
            if isinstance(col, FunctionCall):
                if not col.arg in table_schema:
                    raise ColumnNotFoundError(col.arg, table_name)
            else:
                if col not in table_schema:
                    raise ColumnNotFoundError(col, table_name)
                    
    result = []
    if any(isinstance(col, FunctionCall) for col in ast.columns):
        result_row = {}
        for col in ast.columns:
            if isinstance(col, FunctionCall):
                result_row[col.alias] = execute_function(col, filtered_rows, table_schema)
            result = [result_row]   
    else:
        for row in filtered_rows:
            selected_row = {col: row.get(col) for col in ast.columns}
            result.append(serialize_row(selected_row))
    return result

def condition_evaluation(where, row, table_schema):
    
    if isinstance(where, NegationCondition):
        return not(execute_where_negation_condition(where, row, table_schema))
    
    if isinstance(where, Membership):
        return execute_where_membership_condition(where, row, table_schema)

    if isinstance(where, CheckNullColumn):
        return execute_where_check_nulls(where, row, table_schema)
        
    if isinstance(where, Condition):
       return execute_where_condition(where, row, table_schema)

    elif isinstance(where, LogicalCondition):
        return execute_where_logical_condition(where, row, table_schema)
    
    elif isinstance(where, BetweenCondition):
        if where.NOT:
            return not execute_where_between_condition(where, row, table_schema)
        return execute_where_between_condition(where, row, table_schema)
    
    elif isinstance(where, LikeCondition):
        if where.NOT:
            return not execute_where_like_condition(where, row, table_schema)
        return execute_where_like_condition(where, row, table_schema)
    

def execute_where_condition(where, row, table_schema):
    if (isinstance(row[where.column], VARCHAR) and isinstance(table_schema[where.column](where.value), VARCHAR)) or (isinstance(row[where.column], TEXT) and isinstance(table_schema[where.column](where.value), TEXT)):
            col = row[where.column].value.lower()
            val = table_schema[where.column](where.value).value.lower()
    else:
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

def execute_where_logical_condition(where, row, table_schema):
    MainOperator = where.MainOperator.upper()
    left_result = condition_evaluation(where.left, row, table_schema)
    right_result = condition_evaluation(where.right, row, table_schema)
    
    if MainOperator == "AND":
        return left_result and right_result
    elif MainOperator == "OR":
        return left_result or right_result
    else:
        raise ValueError(f"Unknown logical operator: {MainOperator}")
    
def execute_where_check_nulls(where, row, table_schema):
    value = row.get(where.column).value if issubclass(table_schema[where.column], SQLType) else row.get(where.column)
    if where.isNull:
        return value is None
    else:
        return value is not None

def execute_where_membership_condition(where, row, table_schema):
        value = row.get(where.col).value if issubclass(table_schema[where.col], SQLType) else row.get(where.col)
        if type(value) == str: value = value.lower()
        if where.IN:
            return value in where.args
        else:
            return value not in where.args

def execute_where_negation_condition(where, row, table_schema):
    if isinstance(where.expression, Condition):
        return execute_where_condition(where.expression, row, table_schema)

def execute_where_between_condition(where, row, table_schema):
    col_val = row[where.col]
    if col_val.value == None: return False
    arg1 = where.arg1
    arg2 = where.arg2
    if isinstance(col_val, DATE) and isinstance(table_schema[where.col](arg1), DATE) and isinstance(table_schema[where.col](arg2), DATE):
        arg1 = table_schema[where.col](arg1)
        arg2 = table_schema[where.col](arg2)
        return (arg1 <= col_val <= arg2)
    elif (isinstance(col_val, VARCHAR) or isinstance(col_val, TEXT)) and (isinstance(table_schema[where.col](arg1), VARCHAR) or isinstance(table_schema[where.col](arg1), TEXT)) and (isinstance(table_schema[where.col](arg2), VARCHAR) or isinstance(table_schema[where.col](arg2), TEXT)):
        arg1 = table_schema[where.col](arg1)
        arg2 = table_schema[where.col](arg2)
        return (arg1.value.lower() <= col_val.value.lower() <= arg2.value.lower())
    elif (isinstance(col_val, INT) or isinstance(col_val, FLOAT)) and (isinstance(table_schema[where.col](arg1), INT) or isinstance(table_schema[where.col](arg1), FLOAT)) and (isinstance(table_schema[where.col](arg2), INT) or isinstance(table_schema[where.col](arg2), FLOAT)):   
        arg1 = table_schema[where.col](arg1)
        arg2 = table_schema[where.col](arg2)
        return (arg1 <= col_val <= arg2)
    elif isinstance(col_val, TIME) and isinstance(table_schema[where.col](arg1), TIME) and isinstance(table_schema[where.col](arg2), TIME):
        arg1 = table_schema[where.col](arg1)
        arg2 = table_schema[where.col](arg2)
        return (arg1 <= col_val <= arg2)
    else:
        return False

def execute_where_like_condition(where, row, table_schema):
    val = where.arg
    col_val = row[where.col]
    if col_val.value == None: return False
    if (
        type(val) != str or
         (
            isinstance(table_schema[where.col], VARCHAR) or
            isinstance(table_schema[where.col], TEXT) or
            isinstance(table_schema[where.col], DATE) or
            isinstance(table_schema[where.col], TIME)
        )
    ):
        raise ValueError("LIKE condition only works with string values.")
    val = table_schema[where.col](val).value
    regex = ''
    i = 0
    while i < len(val):
        c = val[i]
        if c == '%':
            regex += '.*'
        elif c == '_':
            regex += '.'
        else:
            regex += re.escape(c)
        i += 1
    regex = '^' + regex + '$'
    return re.fullmatch(regex, col_val.value) is not None
    

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
            print(val)
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


def execute_function(function, rows, table_schema):
    func_name = function.function_name
    arguments = function.arg
    total = 0
    if func_name == "COUNT":
        if arguments == "*":
            return len(rows)
        else:
            return len([row for row in rows if (row.get(arguments).value if isinstance(row.get(arguments), SQLType) else row.get(arguments)) is not None])
    elif func_name == "SUM":
        if table_schema[function.arg] == FLOAT or table_schema[function.arg] == INT:
            for row in rows:
                total += row[function.arg].value
        else:
            raise ValueError(f"SUM Function works Only with INT/FLOAT columns")
        return total
        
            
        
            
        
        
    
    
