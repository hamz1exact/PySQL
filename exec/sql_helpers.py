import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from engine.sql_ast import *  # Import from engine
from errors import *
from sql_types.sql_types import *
from storage.database import Table
from src.constants import *


def serialize_row(row):
    """Convert SQLType objects to raw Python values for display or SELECT output."""
    return {col: (val.value if isinstance(val, SQLType) else val) for col, val in row.items()}

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
    
    elif isinstance(expr, MathFunction):
        return extract_identifiers(expr.expression)
    
    elif isinstance(expr, StringFunction):
        return extract_identifiers(expr.expression)

    return []  # literals, constants, etc.


def get_output_name(col):
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
    
    
def get_expr_output_name(expr):
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
    
def check_duplicate_constraint_violation(table_obj, new_row, columns_being_inserted):
    """
    Check if inserting new_row would violate PRIMARY KEY or UNIQUE constraints.
    
    Args:
        table_obj: Table object with schema, constraints, and rows
        new_row: Dictionary of column_name -> value for the row being inserted
        columns_being_inserted: List of column names that are being explicitly inserted
    
    Returns:
        True if duplicate found (violation), False if no violation
    """
    
    # Check each column that has constraints
    for col_name, constraint in table_obj.constraints.items():
        if constraint in ["PRIMARY KEY", "UNIQUE"]:
            
            # Get the value that will be inserted for this column
            new_value = new_row.get(col_name)
            
            # Skip NULL values - they don't violate uniqueness constraints
            if new_value is None:
                continue
            
            # Extract the actual value if it's wrapped in SQLType
            if hasattr(new_value, 'value'):
                new_value = new_value.value
            
            # Check against existing rows
            for existing_row in table_obj.rows:
                existing_value = existing_row.get(col_name)
                
                # Extract actual value if wrapped in SQLType
                if hasattr(existing_value, 'value'):
                    existing_value = existing_value.value
                
                # Compare values
                if existing_value == new_value:
                    return True  # Duplicate found - violation!
    
    return False  # No violations found
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


def handle_conflict_resolution(ast, violation, table_obj, new_row):
    """
    Handle ON CONFLICT logic
    Returns True if row should be inserted, False if it should be skipped
    """
    if not violation:
        return True  # No conflict, proceed with insert
        
    conflict_col, constraint_type, duplicate_value = violation
    
    if not ast.conflict:
        # No ON CONFLICT clause specified, raise error
        raise ValueError(f"Duplicate value '{duplicate_value}' for {constraint_type} column '{conflict_col}'")
    
    # Validate conflict targets if specified
    if ast.conflict_targets:
        if conflict_col not in ast.conflict_targets:
            raise ValueError(f"There is no unique constraint matching the ON CONFLICT specification. "
                           f"Constraint violation on '{conflict_col}' but targets are {ast.conflict_targets}")
    else:
        # If no specific targets, ON CONFLICT applies to any unique/primary key violation
        pass
    
    # Handle conflict actions
    if ast.action == "NOTHING":
        print(f"Row ignored due to ON CONFLICT DO NOTHING: "
              f"Duplicate value '{duplicate_value}' for {constraint_type} column '{conflict_col}'")
        return False  # Don't insert
        
    elif ast.action == "UPDATE":
        if not ast.update_cols:
            raise ValueError("ON CONFLICT DO UPDATE requires SET clause")
            
        # Update the conflicting row instead of inserting new one
        table_rows = table_obj.rows
        table_schema = table_obj.schema
        
        # Find the existing row that conflicts
        for existing_row in table_rows:
            existing_value = existing_row.get(conflict_col)
            if hasattr(existing_value, 'value'):
                existing_value = existing_value.value
                
            if existing_value == duplicate_value:
                # Update this row
                for col, value in ast.update_cols.items():
                    if col not in table_schema:
                        raise ValueError(f"Unknown column '{col}' in ON CONFLICT DO UPDATE SET")
                    existing_row[col] = table_schema[col](value)
                
                print(f"Row updated due to ON CONFLICT DO UPDATE: "
                      f"Updated existing row with {constraint_type} '{conflict_col}' = '{duplicate_value}'")
                return False  # Don't insert new row, we updated existing
        
        # This shouldn't happen if our constraint detection is correct
        raise ValueError("Internal error: Could not find conflicting row for update")
    
    return True


def find_constraint_violation(table_obj, new_row):
    """
    Returns (column_name, constraint_type, duplicate_value) if violation found, None otherwise
    """
    table_constraints = getattr(table_obj, 'constraints', {})
    table_rows = table_obj.rows
    
    for col_name, constraint in table_constraints.items():
        # Get constraint value (handle both string and object constraints)
        constraint_value = constraint.value if hasattr(constraint, 'value') else constraint
        
        if constraint_value not in ["PRIMARY KEY", "UNIQUE"]:
            continue
            
        # Get new value
        new_value = new_row.get(col_name)
        if hasattr(new_value, 'value'):
            new_value = new_value.value
            
        # Skip NULL values (NULL doesn't violate uniqueness in most SQL systems)
        if new_value is None:
            continue
            
        # Check against existing rows
        for existing_row in table_rows:
            existing_value = existing_row.get(col_name)
            if hasattr(existing_value, 'value'):
                existing_value = existing_value.value
                
            if existing_value == new_value:
                constraint_type = "primary key" if constraint_value == "PRIMARY KEY" else "unique"
                return col_name, constraint_type, new_value
    
    return None



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
    
    elif isinstance(expr, MathFunction):
        return extract_identifiers(expr.expression)
    
    elif isinstance(expr, StringFunction):
        return extract_identifiers(expr.expression)

    return []  # literals, constants, etc.




def get_output_name(col):
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


def get_expr_output_name(expr):
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
