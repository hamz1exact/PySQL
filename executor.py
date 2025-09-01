

def execute(self, ast):
    table_name = ast.table
    if table_name not in Parser.database:
        raise ValueError(f"Table '{table_name}' does not exist")
    table = Parser.database[table_name]
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
        if ast.where is None or self.where_eval(ast.where, row):
            selected_row = {col: row[col] for col in columns_to_return}
            result.append(selected_row)
    return result