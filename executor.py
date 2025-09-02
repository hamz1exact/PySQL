from ast import Condition, LogicalCondition, SelectStatement, InsertStatement, UpdateStatement

def execute(ast, database):
    if isinstance(ast, SelectStatement) or isinstance(ast, LogicalCondition):
        return execute_select_query(ast, database)
    elif isinstance(ast, InsertStatement):
        execute_insert_query(ast, database)
    elif isinstance(ast, UpdateStatement):
        execute_update_query(ast, database)

def execute_select_query(ast, database):
    table_name = ast.table
    if table_name not in database:
        raise ValueError(f"Table '{table_name}' does not exist")
    table = database[table_name][2:]
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

    # Check table existence
    if table_name not in database:
       raise ValueError(f"Table '{table_name}' does not exist")

    table = database[table_name]
    if not table or len(table) < 2:
        raise ValueError(f"Table '{table_name}' is not properly initialized")

    # First row: column types
    class_table = table[0]

    # Second row: default values
    default_row = table[1]

    # Check value count
    if len(values) != len(columns):
        raise ValueError(
            f"Number of values ({len(values)}) does not match number of columns ({len(columns)}). "
            f"Columns: {columns}, Values: {values}"
        )

    new_row = {}

    for col, col_type in class_table.items():
        if col in columns:
            # Get the value provided by user
            idx = columns.index(col)
            val = values[idx]

            # Type conversion
            try:
                if col_type == int:
                    val = int(val)
                elif col_type == bool:
                    if isinstance(val, str):
                        val = val.lower()
                        if val == "true":
                            val = True
                        elif val == "false":
                            val = False
                        else:
                            raise ValueError(f"Invalid boolean value for column '{col}': {val}")
                    else:
                        val = bool(val)
                # Add more types here if needed
            except Exception as e:
                raise ValueError(f"Error converting value for column '{col}': {e}")
            if type(val) != int and type (val) != bool and val.isdigit():
                raise ValueError(
                                f"Invalid value for column '{col}': expected a string (non-numeric), got digits only -> '{val}'")
            new_row[col] = val
        else:
            # Missing value → use default
            if col == "id":
                # Simple auto-increment: max existing id + 1
                existing_ids = [r["id"] for r in table[1:]]  # skip type row
                new_row[col] = max(existing_ids, default=0) + 1
            else:
                new_row[col] = default_row[col]

    table.append(new_row)
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
                row[col] = ast.columns[row]
                cnt += 1
    print(f"{cnt} row(s) updated in '{table_name}'")
