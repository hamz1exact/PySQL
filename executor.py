from sql_ast import *
from database_manager import Table
import re
from datatypes import *
from errors import *
import random
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
    filtered_rows = []
    all_ids = []
    for col in ast.columns:
        all_ids.extend(extract_identifiers(col))
        
    all_where_ids = []
    all_where_ids = extract_identifiers(ast.where)
    for column in all_where_ids:
        if column not in table_schema:
            raise ColumnNotFoundError(column, table_name)


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
    group_by_cols  = []

    for col in ast.group_by:
        group_by_cols.extend(extract_identifiers(col))
    
    if ast.group_by and ast.columns and ast.function_columns:
        for col in all_ids:
            if col not in group_by_cols:
                raise ValueError(f"Column '{col}' must appear in the GROUP BY clause or be used in an aggregate function")
    
    # Filter rows based on WHERE clause
    filtered_rows = []
    for row in table:
        if ast.where is None or ast.where.evaluate(row, table_schema):
            filtered_rows.append(row)
            

    result = []
    
    # Handle GROUP BY queries
    if ast.group_by and ast.function_columns:
        # Build alias-to-expression mapping
        alias_to_expr = {}
        all_select_exprs = (ast.columns or []) + (ast.function_columns or [])
        for expr in all_select_exprs:
            if hasattr(expr, 'alias') and expr.alias:
                alias_to_expr[expr.alias] = expr
        
        # Resolve GROUP BY expressions that might be aliases
        resolved_group_by = []
        for group_expr in ast.group_by:
            if (isinstance(group_expr, ColumnExpression) and 
                group_expr.column_name in alias_to_expr):
                # Use the original expression
                resolved_group_by.append(alias_to_expr[group_expr.column_name])
            else:
                resolved_group_by.append(group_expr)
        groups = {}
        for row in filtered_rows:
            bucket_key = tuple(expr.evaluate(row, table_schema) for expr in resolved_group_by)
            if bucket_key not in groups:
                groups[bucket_key] = []
            groups[bucket_key].append(row)

        # Apply HAVING with proper context
        if ast.having:
            results = {}
            for bucket_key, group_rows in groups.items():
                try:
                    if ast.having.evaluate(group_rows, table_schema):
                        results[bucket_key] = group_rows
                except Exception as e:
                    print(f"Warning: Error evaluating HAVING clause for group {bucket_key}: {e}") 
                    continue
            groups = results
        
        # Build result rows
        for bucket_key, group_rows in groups.items():
            result_row = {}
            
            # Add regular SELECT columns (non-aggregates)
            for col_expr in ast.columns:
                if isinstance(col_expr, Function):
                    continue  # Skip functions, handle below
                
                output_name = col_expr.alias or get_expr_name(col_expr)
                
                # Handle different expression types in GROUP BY context
                if isinstance(col_expr, CaseWhen):
                    # Pass the group to CaseWhen for proper evaluation
                    result_row[output_name] = col_expr.evaluate(group_rows, table_schema)
                else:
                    # Regular expressions use first row
                    result_row[output_name] = col_expr.evaluate(group_rows[0], table_schema)
            
            # Add GROUP BY columns that aren't already in SELECT
            for i, group_expr in enumerate(resolved_group_by):
                group_output_name = group_expr.alias if hasattr(group_expr, "alias") and group_expr.alias else get_expr_name(group_expr)
                
                # Only add if not already covered by SELECT
                already_covered = False
                for col_expr in ast.columns:
                    if isinstance(col_expr, Function):
                        continue
                    if are_same_column(col_expr, group_expr):
                        already_covered = True
                        break
                
                if not already_covered:
                    result_row[group_output_name] = bucket_key[i]
            
            # Add aggregate functions
            for func in ast.function_columns:
                output_name = func.alias or get_expr_name(func)
                result_row[output_name] = func.evaluate(group_rows, table_schema)
            
            result.append(serialize_row(result_row))
    
            
            # Add regular SELECT columns (must be in GROUP BY)
         
    
    # Handle queries with only aggregate functions (no GROUP BY)
    
    elif ast.function_columns and not ast.columns:
        result_row = {}
        for func in ast.function_columns:
            # Aggregate functions need the full list of filtered rows
            result_row[func.alias or get_expr_name(func)] = func.evaluate(filtered_rows, table_schema)
        result.append(serialize_row(result_row))
    
    # Handle mixed aggregate and regular columns without GROUP BY
    elif ast.function_columns and ast.columns:
        raise ValueError("Selected columns must appear in the GROUP BY clause or be used in an aggregate function")
    
    # Handle regular SELECT queries (no aggregates, no GROUP BY)  
      
    else:
        for row in filtered_rows:
            selected_row = {}
            for col in ast.columns:
                # Non-aggregate expressions need single rows
                if isinstance(col, Function):
                    # Even non-aggregate functions should receive proper context
                    selected_row[col.alias or get_expr_name(col)] = col.evaluate([row], table_schema)
                else:
                    # Regular expressions get single row
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
        
        # Build comprehensive mapping of expressions to their output names
        expr_to_output_name = {}
        alias_to_expr = {}
        
        # Map all SELECT expressions to their output names
        all_select_exprs = (ast.columns or []) + (ast.function_columns or [])
        for expr in all_select_exprs:
            output_name = expr.alias or get_expr_name(expr)
            expr_to_output_name[expr] = output_name
            
            if hasattr(expr, 'alias') and expr.alias:
                alias_to_expr[expr.alias] = expr
        
        # Process each ORDER BY clause
        for order_by_clause in ast.order_by:
            expression = order_by_clause.expression
            direction = order_by_clause.direction
            
            if isinstance(expression, ColumnExpression):
                col = expression.column_name
                
                # Strategy 1: Direct column/alias lookup in result
                if col in available_columns:
                    def sort_key(row):
                        value = row.get(col)
                        if value is None:
                            return (0 if direction == "ASC" else 1,)
                        return (1 if direction == "ASC" else 0, value)
                    
                    result = sorted(result, key=sort_key, reverse=(direction == "DESC"))
                    continue
                
                # Strategy 2: Check if it's an alias that maps to an expression
                if col in alias_to_expr:
                    original_expr = alias_to_expr[col]
                    output_name = original_expr.alias or get_expr_name(original_expr)
                    
                    if output_name in available_columns:
                        def sort_key(row):
                            value = row.get(output_name)
                            if value is None:
                                return (0 if direction == "ASC" else 1,)
                            return (1 if direction == "ASC" else 0, value)
                        
                        result = sorted(result, key=sort_key, reverse=(direction == "DESC"))
                        continue
                
                # Strategy 3: For non-GROUP BY queries, allow original table columns
                if not ast.group_by:
                    if col in table_schema:
                        def sort_key(row):
                            # This only works for non-GROUP BY queries where result contains original columns
                            value = row.get(col)
                            if value is None:
                                return (0 if direction == "ASC" else 1,)
                            return (1 if direction == "ASC" else 0, value)
                        
                        result = sorted(result, key=sort_key, reverse=(direction == "DESC"))
                        continue
                
                # If we get here, the column wasn't found
                raise ValueError(f"ORDER BY column '{col}' is not available in the result set. Available columns: {list(available_columns)}")
            
            else:
                # Complex expression (Function, Extract, etc.)
                
                # Strategy 1: Check if this exact expression is in SELECT clause
                matching_output_name = None
                for select_expr in all_select_exprs:
                    if expressions_are_equivalent(expression, select_expr):
                        matching_output_name = select_expr.alias or get_expr_name(select_expr)
                        break
                
                if matching_output_name and matching_output_name in available_columns:
                    def sort_key(row):
                        value = row.get(matching_output_name)
                        if value is None:
                            return (0 if direction == "ASC" else 1,)
                        return (1 if direction == "ASC" else 0, value)
                    
                    result = sorted(result, key=sort_key, reverse=(direction == "DESC"))
                    continue
                
                # Strategy 2: For non-GROUP BY queries, evaluate the expression directly
                if not ast.group_by:
                    def sort_key(row):
                        try:
                            value = expression.evaluate(row, table_schema)
                            if value is None:
                                return (0 if direction == "ASC" else 1,)
                            return (1 if direction == "ASC" else 0, value)
                        except Exception:
                            return (2,)  # Put problematic rows at end
                    
                    result = sorted(result, key=sort_key, reverse=(direction == "DESC"))
                    continue
                
                # If we get here, it's a complex expression in GROUP BY that's not in SELECT
                raise ValueError(f"ORDER BY expression must appear in SELECT clause when using GROUP BY. Available columns: {list(available_columns)}")

    if ast.limit:
        
        if ast.offset:
            result = result[int(ast.offset):]
        result = result[:int(ast.limit)]
    
    return result 



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



def extract_identifiers(expr):
    """Recursively extract all column identifiers from an expression object."""
    
    if isinstance(expr, ColumnExpression):
        return [expr.column_name]

    elif isinstance(expr, BinaryOperation):
        return extract_identifiers(expr.left) + extract_identifiers(expr.right)
    elif isinstance(expr, ConditionExpr):
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
    
    
def get_expr_output_name(expr: Columns):
    if isinstance(expr, LiteralExpression):
        return str(expr.value)
    if expr.alias:
        return expr.alias
    if isinstance(expr, ColumnExpression):
        return expr.column_name
    if isinstance(expr, Cast):
        inner = get_expr_name(expr.expression)
        return f"{expr.name}({inner})" 
    if isinstance(expr, CoalesceFunction):
        res = ""
        for exp in expr.expressions:
            res += get_expr_output_name(exp)
        return res
        

    elif isinstance(expr, BinaryOperation):

        left = get_expr_output_name(expr.left)
        right = get_expr_output_name(expr.right)
        return f"{left}{expr.operator}{right}"
    

def get_expr_name(expr):
    call_id = str(id(expr))
    call_id = call_id[-5:]
    if isinstance(expr, ColumnExpression):
        return expr.column_name
    elif isinstance(expr, LiteralExpression):
        return str(expr.value)
    elif isinstance(expr, BinaryOperation):
        left = get_expr_name(expr.left)
        right = get_expr_name(expr.right)
        return f"{expr.name}.id({call_id})"
    elif isinstance(expr, Function):
        inner = get_expr_name(expr.expression)
        return f"{expr.name}.id({call_id})"
    elif isinstance(expr, MathFunction):
        inner = get_expr_name(expr.expression)
        return f"{expr.name}.id({call_id})"
    elif isinstance(expr, NullIF):
        inner = get_expr_name(expr.expression)
        return f"{expr.name}.id({call_id})"
    elif isinstance(expr, StringFunction):
        inner = get_expr_name(expr.expression)
        return f"{expr.name}.id({call_id})"
    elif isinstance(expr, Replace):
        inner = get_expr_name(expr.expression)
        return f"{expr.name}.id({call_id})"
    elif isinstance(expr, Cast):
        inner = get_expr_name(expr.expression)
        return f"{expr.name}.id({call_id})"
    elif isinstance(expr, Extract):
        inner = get_expr_name(expr.expression)
        return f"{expr.name}.id({call_id})"
    
    
    elif isinstance(expr, DateDIFF):
        # inner1= get_expr_name(expr.date1)
        # inner2= get_expr_name(expr.date2)
        return f"{expr.name}.id({call_id})"
    
    elif isinstance(expr, Concat) or isinstance(expr, CoalesceFunction):
        return f"{expr.name}.id({call_id})"
    else:
        raise ValueError(f"Unknown expression type: {expr}")
    
def are_same_column(expr1, expr2):
    """Check if two expressions refer to the same column or are equivalent"""
    if isinstance(expr1, ColumnExpression) and isinstance(expr2, ColumnExpression):
        return expr1.column_name == expr2.column_name
    
    # Check if they're the same expression type with same parameters
    if type(expr1) == type(expr2):
        if isinstance(expr1, Extract):
            return (expr1.part == expr2.part and 
                    are_same_column(expr1.expression, expr2.expression))
        elif isinstance(expr1, CaseWhen):
            # For CASE expressions, check if they have the same structure
            if len(expr1.expressions) != len(expr2.expressions):
                return False
            for i in range(len(expr1.expressions)):
                if not are_same_column(expr1.expressions[i], expr2.expressions[i]):
                    return False
                if not are_same_column(expr1.actions[i], expr2.actions[i]):
                    return False
            return True
    
    return False

def execute_order_by(result, order_by_clauses, schema):
    """
    result: list of dictionaries (rows)
    order_by_clauses: list of OrderBy objects
    schema: table schema
    """
    if not order_by_clauses or not result:
        return result
    
    # Build set of available columns in result
    available_columns = set(result[0].keys())
    
    for order_by in order_by_clauses:
        expression = order_by.expression
        direction = order_by.direction
        
        if isinstance(expression, ColumnExpression):
            # Simple column ordering
            col = expression.column_name
            if col not in available_columns:
                raise ValueError(f"ORDER BY column '{col}' is not available in the result set")
            
            result = sorted(result, key=lambda row: row.get(col), 
                          reverse=(direction == "DESC"))
        
        else:
            # Complex expression ordering (functions, math, etc.)
            result = sorted(result, 
                          key=lambda row: expression.evaluate(row, schema), 
                          reverse=(direction == "DESC"))
    
    return result


# Replace your ORDER BY section in execute_select_query with this:


# Helper function to check if two expressions are equivalent
def expressions_are_equivalent(expr1, expr2):
    """Check if two expressions are functionally equivalent"""
    
    # Same type check
    if type(expr1) != type(expr2):
        return False
    
    # Handle different expression types
    if isinstance(expr1, ColumnExpression):
        return expr1.column_name == expr2.column_name
    
    elif isinstance(expr1, LiteralExpression):
        return expr1.value == expr2.value
    
    elif isinstance(expr1, Function):
        return (expr1.name == expr2.name and 
                expr1.distinct == expr2.distinct and
                expressions_are_equivalent(expr1.expression, expr2.expression))
    
    elif isinstance(expr1, Extract):
        return (expr1.part == expr2.part and
                expressions_are_equivalent(expr1.expression, expr2.expression))
    
    elif isinstance(expr1, Cast):
        return (expr1.target_type == expr2.target_type and
                expressions_are_equivalent(expr1.expression, expr2.expression))
    
    elif isinstance(expr1, MathFunction):
        return (expr1.name == expr2.name and
                expr1.round_by == expr2.round_by and
                expressions_are_equivalent(expr1.expression, expr2.expression))
    
    elif isinstance(expr1, BinaryOperation):
        return (expr1.operator == expr2.operator and
                expressions_are_equivalent(expr1.left, expr2.left) and
                expressions_are_equivalent(expr1.right, expr2.right))
    
    elif isinstance(expr1, Concat):
        if len(expr1.expressions) != len(expr2.expressions):
            return False
        return all(expressions_are_equivalent(e1, e2) 
                  for e1, e2 in zip(expr1.expressions, expr2.expressions))
    
    # Add more cases as needed for other expression types
    else:
        # Fallback: compare string representations
        return str(expr1) == str(expr2)
