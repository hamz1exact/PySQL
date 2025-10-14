import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))
from exec.sql_helpers import *


def execute_select_query(ast, db_manager):

    
    # Hamdle Non From Queries
    if not ast.table:
        res_row = {}
        for col in ast.columns:
            col_id = str(id(col))
            col_id = f"?col?.id({col_id[-5:]})"
            res_row[col.alias if col.alias else col_id] = col.evaluate({}, {})
        return [res_row]
    

    # Handle CTEs and Subqueries
    
    if isinstance(ast.table.table_name, SelectStatement):

        table_name = ast.table.alias if ast.table.alias else "subquery"
        table = ast.table.table_name.evaluate()
        table_schema = generate_schema(table)
    elif isinstance(ast.table.table_name, str):

        table_name = ast.table.table_name
        database = db_manager.active_db
        
        if table_name not in database and table_name not in db_manager.views:
            raise ValueError(f"Table '{table_name}' does not exist")
        elif table_name in database:
            table = database[table_name].rows
            table_schema = database[table_name].schema
        elif table_name in db_manager.views:
            table = db_manager.views[table_name].evaluate()
            table_schema = generate_schema(table)
    else:
        
        table_name = str(ast.table.table_name)
        database = db_manager.active_db
        
        if table_name not in database and table_name not in db_manager.views:
            raise ValueError(f"Table '{table_name}' does not exist")
        elif table_name in database:
            table = database[table_name].rows
            table_schema = database[table_name].schema
        elif table_name in db_manager.views:
            table = db_manager.views[table_name].evaluate()
            table_schema = generate_schema(table)

    
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
            
            for key in table_schema.keys():
                normalized_columns.append(ColumnExpression(key, key))
        elif isinstance(col, ColumnExpression):
            
            if col.column_name not in table_schema:
                raise ColumnNotFoundError(col.column_name, table_name)

            alias = f"{table_name}.{col.column_name}" if table_has_asterisk else col.alias
            normalized_columns.append(ColumnExpression(col.column_name, alias))
        else:
            
            normalized_columns.append(col)

    ast.columns = normalized_columns
    
    all_ids = []
    
    for col in ast.columns:
        all_ids.extend(extract_identifiers(col))
    
    
    
    if table_has_asterisk and ast.function_columns:
        raise ValueError ("Aggregation over all grouped columns is redundant. The result will return the same value as the original rows. Remove unnecessary aggregates or reduce the GROUP BY columns.")
    

    function_args = []
    
    for col in ast.function_columns:
        function_args.extend(extract_identifiers(col))
    
    
    
    for col in function_args:
        if col != "*" and col not in table_schema:
            raise ColumnNotFoundError(col, table_name)
    

    group_by_cols  = []
    
    if ast.group_by:
        for col in ast.group_by:
            group_by_cols.extend(extract_identifiers(col))
        
    if ast.group_by and (ast.columns or ast.function_columns):
        
        for col in all_ids:
            if col != "*":  
                if col not in group_by_cols:
                    is_regular_column = False
                    for select_expr in ast.columns:
                        expr_cols = extract_identifiers(select_expr)
                        if col in expr_cols:
                            is_regular_column = True
                            break
                    
                    if is_regular_column:
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


