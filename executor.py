from sql_ast import *
from database_manager import Table
import re
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
    
    all_ids = []
    for col in ast.columns:
        all_ids.extend(extract_identifiers(col))

    table_has_asterisk = "*" in all_ids
    normalized_columns = []

    for col in ast.columns:
        if isinstance(col, ColumnExpression) and col.column_name == "*":
            # Expand *
            for key in table_schema.keys():
                normalized_columns.append(ColumnExpression(key, key))
        elif isinstance(col, ColumnExpression):
            # Validate normal columns
            if col.column_name not in table_schema:
                raise ColumnNotFoundError(col.column_name, table_name)

            alias = f"{table_name}.{col.column_name}" if table_has_asterisk else col.alias
            normalized_columns.append(ColumnExpression(col.column_name, alias))
        else:
            # This is an expression (BinaryOp, FunctionCall, etc.)
            normalized_columns.append(col)

    ast.columns = normalized_columns
    
    all_ids = []
    
    for col in ast.columns:
        all_ids.extend(extract_identifiers(col))

    
    if table_has_asterisk and ast.function_columns:
        raise ValueError ("Aggregation over all grouped columns is redundant. The result will return the same value as the original rows. Remove unnecessary aggregates or reduce the GROUP BY columns.")
    
    # Validate special columns (aggregate functions)
    function_args = []
    for col in ast.function_columns:
        function_args.extend(extract_identifiers(col))
    
    for col in function_args:
        if col != "*" and col not in table_schema:
            raise ColumnNotFoundError(col, table_name)
    
    # Check GROUP BY constraint: selected columns must appear in GROUP BY or be aggregates
    if ast.group_by and ast.columns and ast.function_columns:
        for col in all_ids:
            if col not in set(ast.group_by):
                raise ValueError(f"Column '{col}' must appear in the GROUP BY clause or be used in an aggregate function")
    
    # Filter rows based on WHERE clause
    filtered_rows = []
    for row in table:
        if ast.where is None or where_condition_evaluation(ast.where, row, table_schema):
            filtered_rows.append(row)
    
    result = []
    
    # Handle GROUP BY queries
    if ast.group_by and ast.function_columns and ast.columns:
        
        groups = {}
        for row in filtered_rows:
            # Create tuple key from GROUP BY column values
            bucket_key = tuple(getattr(row[col], 'value', None) for col in ast.group_by)
            if bucket_key not in groups:
                groups[bucket_key] = []
            groups[bucket_key].append(row)
        

        
        if ast.having:
            results = {}
            for bucket_key, group_rows in groups.items():
                if having_condition_evaluation(ast.having, ast.group_by ,group_rows, table_schema):
                    results[bucket_key] = group_rows
            groups = results
        
            
        ()
        # Build result rows for each group
        for bucket_key, group_rows in groups.items():
            result_row = {}
            
            # Add GROUP BY columns
            for i, col in enumerate(ast.group_by):
                ok = False
                for object in ast.columns:
                    custom_columns = "".join(extract_identifiers(object))
                    if custom_columns == col:
                        ok = True
                if ok:
                    result_row[object.alias if alias else get_expr_name(object)] = bucket_key[i]
                else:
                    result_row[col] = bucket_key[i]
                
        
            
            # Add regular SELECT columns (must be in GROUP BY)
            for col in ast.columns:
                column_name = "".join(extract_identifiers(col))
                if column_name not in result_row:
                    # Evaluate the expression for the first row in the group, or None if empty
                    result_row[col.alias or get_expr_name(col)] = col.evaluate(group_rows[0], table_schema) if group_rows else None

                if ast.group_by and col in ast.group_by.expressions:
                    # Already included via GROUP BY
                    pass
                else:
                    # If not in GROUP BY, include it anyway (validation ensures this is safe)
                    result_row[col.alias or get_expr_name(col)] = col.evaluate(group_rows[0], table_schema) if group_rows else None
            # Add aggregate function results
            for func in ast.function_columns:
                
                result_row[func.alias or get_expr_name(func)] = func.evaluate(group_rows, table_schema)
            
            result.append(serialize_row(result_row))
    
    # Handle queries with only aggregate functions (no GROUP BY)
    elif ast.function_columns and not ast.columns:
        result_row = {}
        # Add all aggregate function results to the same row
        for func in ast.function_columns:
            result_row[func.alias or get_expr_name(func)] = func.evaluate(filtered_rows, table_schema)
        result.append(serialize_row(result_row))
    
    # Handle mixed aggregate and regular columns without GROUP BY
    elif ast.function_columns and ast.columns:
        raise ValueError("Selected columns must appear in the GROUP BY clause or be used in an aggregate function")
    
    # Handle regular SELECT queries (no aggregates, no GROUP BY)    
    else:
        
        for row in filtered_rows:
            selected_row = {}
            # selected_row = {col.alias if col.alias else col.col_object: row.get(col.col_object) for col in ast.columns}
            for col in ast.columns:
                selected_row[col.alias or get_expr_name(col)] = col.evaluate(row, table_schema) 
            result.append(serialize_row(selected_row))
    
    # Apply DISTINCT if specified
    
    if ast.distinct:
        
        if ast.columns:
            column_names = [get_expr_name(col) for col in ast.columns]
        else:
            column_names = list(result[0].keys())  

        # Then build the row tuples using actual evaluated values
        seen = set()
        unique_rows = []
        for row in result:
            row_tuple = tuple(row[name] for name in column_names)
            if row_tuple not in seen:
                seen.add(row_tuple)
                unique_rows.append(row)
        result = unique_rows
    
    # Apply ORDER BY if specified
    if ast.order_by:
        
        # Build set of available columns in result
        available_columns = set()
        if result:
            available_columns = set(result[0].keys())
        
        for col, direction in ast.order_by:
            if col not in available_columns:
                raise ValueError(f"ORDER BY column '{col}' is not available in the result set")
            
            # Sort by this column
            result = sorted(result, key=lambda row: row.get(col), reverse=(direction == "DESC"))
    if ast.limit:
        
        if ast.offset:
            result = result[int(ast.offset):]
        result = result[:int(ast.limit)]
    
    return result


def having_condition_evaluation(having, group, row, table_schema):
    
    if isinstance(having, HavingCondition):
        return execute_having_condition(having, group, row, table_schema)

    elif isinstance(having, HavingLogicalCondition):
        return execute_having_logical_condition(having, group, row, table_schema)
    
    
def execute_having_condition(having, group, row, table_schema):
        if having.left.type == "FUNC":
            left = execute_function(having.left.content, row, table_schema)
        elif having.left.type == "VALUE":
            left = having.left.content
        elif having.left.type == "ID":
            left = execute_having_col_condition(having.left.content, group, row)
        
        if having.right.type == "FUNC":
            right = execute_function(having.right.content, row, table_schema)
        elif having.right.type == "VALUE":
            right = having.right.content
        elif having.right.type == "ID":
            right = execute_having_col_condition(having.right.content, group, row)
        
        if having.left.type == "VALUE":
            if type(having.left.content) == str:
                left = str(left).lower()
        if having.right.type == "VALUE":
            if type(having.right.content) == str:
                right = str(right).lower()
        if having.low_operator == "=": return left == right
        if having.low_operator == "!=": return left != right
        if having.low_operator == "<": return left < right
        if having.low_operator == "<=": return left <= right
        if having.low_operator == ">": return left > right
        if having.low_operator == ">=": return left >= right
        
def execute_having_logical_condition(having, group, row, table_schema):
    high_operator = having.high_operator
    left_expression_result = having_condition_evaluation(having.left_expression ,group, row, table_schema)
    right_expression_result = having_condition_evaluation(having.right_expression ,group, row, table_schema)
    
    if high_operator == "AND":
        return left_expression_result and right_expression_result
    else:
        return left_expression_result or right_expression_result

def execute_having_col_condition(col, group, row):
    if col not in set(group):
        raise ValueError(f"column {col} must appear in the GROUP BY clause or be used in an aggregate function")
    row = row[0]
    if type(row[col].value) == str:
        return row[col].value.lower()
    return row[col].value


def where_condition_evaluation(where, row, table_schema):
    
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
    left_result = where_condition_evaluation(where.left, row, table_schema)
    right_result = where_condition_evaluation(where.right, row, table_schema)
    
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
    for col_object, col_val in table_schema.items():
        if col_object in columns:
            idx = columns.index(col_object)
            val = values[idx]
            print(val)
            new_row[col_object] = table_schema[col_object](val)
        elif col_object in table_auto:
            new_row[col_object] = table_auto[col_object].next()
        elif col_object in table_default:
            new_row[col_object] = table_default[col_object]
        else:
            new_row[col_object] = None     
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
        if ast.where is None or where_condition_evaluation(ast.where, row, table_schema):
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
        if where_condition_evaluation(ast.where, row, table_schema):
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
    values = None
    if function.arg == '*':
        values = rows
    else:
            # Extract column values, skip None rows safely
            values = [row[function.arg].value for row in rows if row[function.arg] is not None]
    total = 0
    if function.distinct:
        values = list(set(values))

    # Execute function
    if func_name == "COUNT":

        return len(values)
    elif func_name == "SUM":
        if arguments == "*":
            raise ValueError(f"'*' Not Supported in SUM Function")
        if table_schema[function.arg] == FLOAT or table_schema[function.arg] == INT:
            total = sum(values)
        else:
            raise ValueError(f"SUM Function works Only with INT/FLOAT columns")
        return float(f"{total:.2f}")
    elif func_name == "MIN":
        if arguments == "*":
            raise ValueError(f"'*' Not Supported in MIN Function")
        if table_schema[function.arg] == FLOAT or table_schema[function.arg] == INT:
            total = min(values)
        else:
            raise ValueError(f"MIN Function works Only with INT/FLOAT columns")
        return total
    elif func_name == "MAX":
        if arguments == "*":
            raise ValueError(f"'*' Not Supported in MAX Function")
        if table_schema[function.arg] == FLOAT or table_schema[function.arg] == INT:
            total = max(values)
        else:
            raise ValueError(f"MAX Function works Only with INT/FLOAT columns")
        return total
    elif func_name == "AVG":
        if arguments == "*":
            raise ValueError(f"'*' Not Supported in AVG Function")
        if table_schema[function.arg] == FLOAT or table_schema[function.arg] == INT:
            if not values:  # Handle empty case
                return 0.0
            total = sum(values) / len(values)  # Fixed: use len(values)
            return float(f"{total:.2f}")  # Add return statement and consistent formatting
        else:
            raise ValueError(f"AVG Function works Only with INT/FLOAT columns")
                    
def extract_identifiers(expr):
    """Recursively extract all column identifiers from an expression object."""
    
    if isinstance(expr, ColumnExpression):
        return [expr.column_name]

    elif isinstance(expr, BinaryOperation):
        return extract_identifiers(expr.left) + extract_identifiers(expr.right)

    elif isinstance(expr, Function):
        ids = []
        ids.extend(extract_identifiers(expr.expression))
        return ids

    elif isinstance(expr, Columns):  
        # in case you want to pass a Columns wrapper
        return extract_identifiers(expr.col_object)

    return []  # literals, constants, etc.


def get_output_name(col: Columns):
    if col.alias:
        return col.alias
    obj = col.col_object
    if isinstance(obj, ColumnExpression):
        return obj.column_name
    elif isinstance(obj, LiteralExpression):
        return str(obj.value)
    elif isinstance(obj, BinaryOperation):
        # recursively stringify left and right
        left = get_expr_name(obj.left)
        right = get_expr_name(obj.right)
        return f"{left}{obj.operator}{right}"

def get_expr_name(expr):
    if isinstance(expr, ColumnExpression):
        return expr.column_name
    elif isinstance(expr, LiteralExpression):
        return str(expr.value)
    elif isinstance(expr, BinaryOperation):
        left = get_expr_name(expr.left)
        right = get_expr_name(expr.right)
        return f"{left}{expr.operator}{right}"
    elif isinstance(expr, Function):
        inner = get_expr_name(expr.expression)
        return f"{expr.name}({inner})"