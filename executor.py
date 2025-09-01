from ast import Condition, LogicalCondition
def execute(ast, database):
    table_name = ast.table
    if table_name not in database:
        raise ValueError(f"Table '{table_name}' does not exist")
    table = database[table_name]
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
        if ast.where is None or where_eval(ast.where, row):
            selected_row = {col: row[col] for col in columns_to_return}
            result.append(selected_row)
    return result


def where_eval(where, row):
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
        MainOperator = where.MainOperator
        left = where_eval(where.left, row)
        right = where_eval(where.right, row)
        return (left and right if MainOperator.upper() == "AND" else (left or right))
