# Updated utilities.py - Remove duplicate execute function and keep only helper functions

from storage.database import DatabaseManager, Table
from errors import *
from engine.sql_ast import *
from sql_types.sql_types import *


db_manager = DatabaseManager()

def extract_identifiers(expr):
    """Recursively extract all column identifiers from an expression object."""
    
    if expr is None:
        return []
        
    if isinstance(expr, ColumnExpression):
        return [expr.column_name]
    elif isinstance(expr, QualifiedColumnExpression):
        return [expr.column_name]
    elif isinstance(expr, SelectStatement):
        # Extract identifiers from subquery
        ids = []
        if expr.columns:
            for col in expr.columns:
                ids.extend(extract_identifiers(col))
        if expr.function_columns:
            for col in expr.function_columns:
                ids.extend(extract_identifiers(col))
        if expr.where:
            ids.extend(extract_identifiers(expr.where))
        if expr.group_by:
            for col in expr.group_by:
                ids.extend(extract_identifiers(col))
        if expr.having:
            ids.extend(extract_identifiers(expr.having))
        return ids
    elif isinstance(expr, BinaryOperation):
        return extract_identifiers(expr.left) + extract_identifiers(expr.right)
    elif isinstance(expr, ConditionExpr):
        return extract_identifiers(expr.left) + extract_identifiers(expr.right)
    elif isinstance(expr, Function):
        return extract_identifiers(expr.expression)
    elif isinstance(expr, MathFunction):
        return extract_identifiers(expr.expression)
    elif isinstance(expr, StringFunction):
        return extract_identifiers(expr.expression)
    elif isinstance(expr, Cast):
        return extract_identifiers(expr.expression)
    elif isinstance(expr, Extract):
        return extract_identifiers(expr.expression)
    elif isinstance(expr, CaseWhen):
        ids = []
        for exp in expr.expressions:
            ids.extend(extract_identifiers(exp))
        for action in expr.actions:
            ids.extend(extract_identifiers(action))
        if expr.case_else:
            ids.extend(extract_identifiers(expr.case_else))
        return ids
    elif isinstance(expr, Between):
        ids = extract_identifiers(expr.expression)
        ids.extend(extract_identifiers(expr.lower))
        ids.extend(extract_identifiers(expr.upper))
        return ids
    elif isinstance(expr, Membership):
        ids = extract_identifiers(expr.col)
        for arg in expr.args:
            ids.extend(extract_identifiers(arg))
        return ids
    elif isinstance(expr, LikeCondition):
        ids = extract_identifiers(expr.expression)
        ids.extend(extract_identifiers(expr.pattern_expression))
        return ids
    elif isinstance(expr, IsNullCondition):
        return extract_identifiers(expr.expression)
    elif isinstance(expr, NegationCondition):
        return extract_identifiers(expr.expression)
    elif isinstance(expr, Exists):
        return extract_identifiers(expr.subquery)
    elif isinstance(expr, Concat):
        ids = []
        for exp in expr.expressions:
            ids.extend(extract_identifiers(exp))
        return ids
    elif isinstance(expr, CoalesceFunction):
        ids = []
        for exp in expr.expressions:
            ids.extend(extract_identifiers(exp))
        return ids
    elif isinstance(expr, Replace):
        ids = extract_identifiers(expr.expression)
        ids.extend(extract_identifiers(expr.old))
        ids.extend(extract_identifiers(expr.new))
        return ids
    elif isinstance(expr, NullIF):
        ids = extract_identifiers(expr.expression)
        ids.extend(extract_identifiers(expr.number))
        return ids
    elif isinstance(expr, DateDIFF):
        ids = extract_identifiers(expr.date1)
        ids.extend(extract_identifiers(expr.date2))
        return ids

    return []  # literals, constants, etc.

def print_table(rows):
    """Pretty print table results"""
    if not rows:
        print("Empty result")
        return

    # Get columns from the first row
    columns = list(rows[0].keys())

    # Calculate column widths
    col_widths = {col: len(col) for col in columns}
    for row in rows:
        for col in columns:
            col_widths[col] = max(col_widths[col], len(str(row[col])))

    # Print header
    header = " | ".join(col.ljust(col_widths[col]) for col in columns)
    print(header)
    print("-" * len(header))

    # Print rows
    for row in rows:
        line = " | ".join(str(row[col]).ljust(col_widths[col]) for col in columns)
        print(line)
        
        
def create_temp_cte_table(ast, db_manager):
    cte_rows = ast.query.evaluate()
    cte_schema = generate_schema(cte_rows)
    new_table = Table(
        name=ast.table_name, 
        schema=cte_schema, 
        defaults={}, 
        auto={}, 
        constraints={}, 
        restrictions={}, 
        private_constraints={}, 
        constraints_ptr={}
    )
    for row in cte_rows:
        converted_rows = {}
        for col, val in row.items():
            if val is None:
                converted_rows[col] = None
            else:
                try:
                    sql_type_class = cte_schema[col]
                    converted_rows[col] = sql_type_class(val)
                except Exception as e:
                    print(f"Error converting value {val} ({type(val)}) to {sql_type_class.__name__} for column '{col}': {e}")
                    converted_rows[col] = VARCHAR(str(val))
        new_table.rows.append(converted_rows)
    db_manager.active_db[ast.cte_name] = new_table
    
def generate_schema(rows):
    schema = {}
    column_samples = {}
    
    # Collect samples from all rows to better determine types
    for row in rows:
        for col, val in row.items():
            if col not in column_samples:
                column_samples[col] = []
            if val is not None:  # Only collect non-null samples
                column_samples[col].append(val)
    
    # Determine schema based on samples
    for col, samples in column_samples.items():
        if not samples:  # All values were None
            schema[col] = VARCHAR
            continue
            
        # Check the first few non-null values to determine type
        sample_val = samples[0]
        
        # Important: Check bool BEFORE int because isinstance(True, int) returns True in Python!
        if isinstance(sample_val, bool):
            schema[col] = BOOLEAN
        elif isinstance(sample_val, int):
            schema[col] = INT
        elif isinstance(sample_val, float):
            schema[col] = FLOAT
        elif isinstance(sample_val, str):
            schema[col] = VARCHAR
        elif isinstance(sample_val, datetime):
            schema[col] = TIMESTAMP
        elif isinstance(sample_val, date):
            schema[col] = DATE
        elif isinstance(sample_val, time):
            schema[col] = TIME
        else:
            schema[col] = VARCHAR
    return schema