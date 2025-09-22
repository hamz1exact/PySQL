# Updated utilities.py - Remove duplicate execute function and keep only helper functions

from database_manager import DatabaseManager, Table
from datatypes import *
from errors import *
from sql_ast import *

# Keep only the database manager instance
db_manager = DatabaseManager()

# Remove the duplicate execute function - it should only be in executor.py
# Remove execute_select_query, execute_insert_query, etc. - they're in executor.py

# Keep only the helper functions that are actually used:

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