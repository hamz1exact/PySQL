import errors
from datatypes import *
import math
import re

class SelectStatement:
    
    __slots__ = ['columns', 'function_columns', 'table', 'where', 'distinct', 
                 'order_by', 'group_by', 'having', 'offset', 'limit']
    
    def __init__(self, columns = None, function_columns = None, table = None, where = None, distinct = False, order_by = None, group_by = None, having = None, offset = None, limit = None):
        
        self.columns = columns
        self.function_columns = function_columns
        self.table = table
        self.where = where        
        self.distinct = distinct
        self.order_by = order_by
        self.group_by = group_by
        self.having = having
        self.offset = offset
        self.limit = limit
        
class Columns:
    def __init__(self, col_object, alias = None):
        self.col_object = col_object
        self.alias = alias
        

class Condition:
    def __init__(self, column, operator, value):
        self.column = column
        self.operator = operator
        self.value = value
            
            
class LogicalCondition:
    def __init__(self, left, MainOperator, right):
        self.left = left
        self.MainOperator = MainOperator
        self.right = right

class InsertStatement:
    def __init__(self, table, columns, values):
        self.table = table
        self.columns = columns
        self.values = values
        
class UpdateStatement:
    def __init__(self, table, columns, where):
        self.table = table
        self.columns = columns
        self.where = where

class DeleteStatement:
    def __init__(self, table, where = None):
        self.table = table
        self.where = where
        
class CreateDatabseStatement:
    def __init__(self, database_name):
        self.database_name = database_name
        
class CreateTableStatement:
    def __init__(self, table_name, schema, defaults, auto):
        self.table_name = table_name
        self.schema = schema
        self.defaults = defaults
        self.auto = auto

class UseStatement:
    def __init__(self, database_name):
        self.database_name = database_name
        
        
class FunctionCall:
    def __init__(self, function_name, arg, distinct = False, alias = "?column?"):
        self.function_name = function_name
        self.alias = alias
        self.arg = arg
        self.distinct = distinct
        
class CheckNullColumn:
    def __init__(self, column, isNull = True):
        self.column = column
        self.isNull = isNull

class Membership:
    def __init__(self, col, args, IN = True):
        self.col = col
        self.IN = IN
        self.args = args

class NegationCondition:
    def __init__(self, expression):
        self.expression = expression
        
class BetweenCondition:
    def __init__(self, col, arg1, arg2, NOT = False):
        self.col = col
        self.NOT = NOT
        self.arg1 = arg1
        self.arg2 = arg2

class LikeCondition:
    def __init__(self, col, arg, NOT = False):
        self.col = col
        self.arg = arg
        self.NOT = NOT
        
class HavingCondition:
    def __init__(self, left, low_operator, right):
        self.left = left
        self.low_operator = low_operator
        self.right = right
        
class HavingLogicalCondition:
    def __init__(self, left_expression, high_operator, right_expression):
        self.left_expression = left_expression
        self.high_operator = high_operator
        self.right_expression = right_expression
        
class InpType:
    def __init__(self, type, content):
        self.type = type
        self.content = content

      

class Expression:

    def evaluate(self, row, schema):
        raise NotImplementedError
    
    def get_referenced_columns(self):

        raise NotImplementedError

 
class ColumnExpression(Expression):
    def __init__(self, column_name, alias = None):
        self.column_name = column_name
        self.alias = alias
    
    def evaluate(self, row, schema):
        # Handle the case where row might be a string or other unexpected type
        if not isinstance(row, dict):
            raise TypeError(f"ColumnExpression.evaluate expects a dictionary (row), got {type(row)}: {row}")
            
        if self.column_name == "*":
            return row
        
        if self.column_name not in row:
            raise KeyError(f"Column '{self.column_name}' not found in row")
        
        value = row[self.column_name]
        if isinstance(value, SQLType):
            return value.value
        else:
            return value

    def get_referenced_columns(self):
        return {self.column_name}

class LiteralExpression(Expression):
    def __init__(self, value):
        self.value = value
    
    def evaluate(self, row, schema, expected_type = None):
        
        if expected_type is not None and issubclass(expected_type, SQLType):
            converted_value = expected_type(self.value).value            
            return converted_value
        return self.value
    
    def get_referenced_columns(self):
        return set()

class BinaryOperation(Expression):
    def __init__(self, left, operator, right, alias = None):
        self.left = left        
        self.operator = operator 
        self.right = right      
        self.alias = alias
    
    def evaluate(self, row, schema):
        left_val = self.left.evaluate(row, schema)
        
        right_val = self.right.evaluate(row, schema)
        
        if left_val is None or right_val is None:
            return None
        
        if self.operator == '+':
            return left_val + right_val
        elif self.operator == '-':
            return left_val - right_val
        elif self.operator == '*':
            return left_val * right_val
        elif self.operator == '/':
            return left_val / right_val
        else:
            raise ValueError(f"Unknown operator: {self.operator}")
    
    def get_referenced_columns(self):
        return self.left.get_referenced_columns() | self.right.get_referenced_columns()
    

class Function(Expression):
    def __init__(self, name, expression, alias=None, distinct=False):
        self.name = name
        self.expression = expression
        self.alias = alias
        self.distinct = distinct

    def evaluate(self, rows, table_schema):
        if not rows:  # Handle empty group
            return None
        
        # Ensure rows is a list
        if not isinstance(rows, list):
            rows = [rows]
        
        # Evaluate the expression for each row
        values = []
        for row in rows:
            try:
                # Make sure each row is a dictionary
                if not isinstance(row, dict):
                    continue
                    
                value = self.expression.evaluate(row, table_schema)
                if value is not None:  # Skip None values
                    values.append(value)
            except (KeyError, AttributeError, TypeError):
                # Skip rows where evaluation fails
                continue
        
        # Handle DISTINCT
        if self.distinct:
            if values and isinstance(values[0], dict):
                raise ValueError("'*' Not Supported In This Expression")
            values = list(set(values))

        # Handle empty values
        if not values:
            if self.name == "COUNT":
                return 0
            else:
                return None

        # Execute function
        if self.name == "COUNT":
            return len(values)
        elif self.name == "SUM":
            if not all(isinstance(v, (int, float)) for v in values):
                raise ValueError("SUM works only with numeric values")
            return sum(values)
        elif self.name == "AVG":
            if not all(isinstance(v, (int, float)) for v in values):
                raise ValueError("AVG works only with numeric values")
            return sum(values) / len(values)
        elif self.name == "MAX":
            return max(values)
        elif self.name == "MIN":
            return min(values)
        else:
            raise ValueError(f"Unknown function: {self.name}")

class GroupBy(Expression):
    def __init__(self, expressions):
        self.expressions = expressions


class ConditionExpr(Expression):
    def __init__(self, left, operator, right, context):
        self.left = left
        self.operator = operator
        self.right = right
        self.context = context # new att context
        

    def evaluate(self, row_or_rows, schema):
        if self.context == "HAVING":
            # For HAVING, we need to handle aggregates vs regular columns differently
            left = self._evaluate_having_expr(self.left, row_or_rows, schema)
            right = self._evaluate_having_expr(self.right, row_or_rows, schema)
        else:
            # For WHERE, evaluate normally with single row
            left = self._evaluate_where_expr(self.left, row_or_rows, schema)
            right = self._evaluate_where_expr(self.right, row_or_rows, schema)

        # Handle None values in comparisons
        if left is None or right is None:
            if self.operator == '=':
                return left is None and right is None
            elif self.operator == '!=':
                return left is not None or right is not None
            else:
                return False  # Other operators with NULL values return False

        # Handle string comparisons (case-insensitive)
        if isinstance(right, str) and isinstance(left, str):
            left = left.lower()
            right = right.lower()
        
        # Comparison operations
        if self.operator == '=': 
            return left == right
        elif self.operator == ">": 
            return left > right
        elif self.operator == "<": 
            return left < right
        elif self.operator == "<=": 
            return left <= right
        elif self.operator == ">=": 
            return left >= right
        elif self.operator == "!=": 
            return left != right
        elif self.operator == "AND": 
            return left and right
        elif self.operator == "OR": 
            return left or right
        else:
            raise ValueError(f"Unknown operator: {self.operator}")
    def _evaluate_having_expr(self, expr, group_rows, schema):
        """Evaluate expression in HAVING context (with group of rows)"""
        if not group_rows:  # Handle empty groups
            return None
            
        if isinstance(expr, Function):
            # Aggregate functions need the full group
            return expr.evaluate(group_rows, schema)
        elif isinstance(expr, ColumnExpression):
            # Regular columns: use value from first row (all rows in group have same GROUP BY values)
            return expr.evaluate(group_rows[0], schema)
        elif isinstance(expr, LiteralExpression):
            # Literals evaluate the same regardless of context
            return expr.evaluate({}, schema)
        elif isinstance(expr, BinaryOperation):
            # For binary operations, evaluate each side appropriately
            left = self._evaluate_having_expr(expr.left, group_rows, schema)
            right = self._evaluate_having_expr(expr.right, group_rows, schema)
            
            if left is None or right is None:
                return None
                
            if expr.operator == '+':
                return left + right
            elif expr.operator == '-':
                return left - right
            elif expr.operator == '*':
                return left * right
            elif expr.operator == '/':
                return left / right
            else:
                raise ValueError(f"Unknown operator: {expr.operator}")
        elif isinstance(expr, ConditionExpr):
            # Handle condition expressions in HAVING
            return expr.evaluate(group_rows, schema)
        else:
            # For other expression types, try normal evaluation with first row
            return expr.evaluate(group_rows[0], schema)
    
    def _evaluate_where_expr(self, expr, row, schema):
        """Evaluate expression in WHERE context (with single row)"""
        # Handle type coercion for literals
        if isinstance(expr, LiteralExpression):
            # Try to get expected type from the other side of comparison
            other_expr = self.right if expr == self.left else self.left
            if isinstance(other_expr, ColumnExpression):
                expected_type = schema[other_expr.column_name]
                return expr.evaluate(row, schema, expected_type)
        
        return expr.evaluate(row, schema)
        

class Between(Expression):
    def __init__(self, expression, lower, upper, is_not = False):
        self.expression = expression
        self.lower = lower
        self.upper = upper
        self.is_not = is_not

    def evaluate(self, row, schema):
        if isinstance(self.expression, ColumnExpression):
            expected_type = schema[self.expression.column_name]
        else:
            expected_type = None
        expr_value = self.expression.evaluate(row, schema)
        lower_value = self.lower.evaluate(row, schema, expected_type)
        upper_value = self.upper.evaluate(row, schema, expected_type)
        if expr_value is None or lower_value is None or upper_value is None:
            return False
        if not self.is_not:
            return self.lower.evaluate(row, schema, expected_type = expected_type)<=self.expression.evaluate(row, schema)<=self.upper.evaluate(row, schema, expected_type = expected_type)
        return not self.lower.evaluate(row, schema, expected_type = expected_type)<=self.expression.evaluate(row, schema)<=self.upper.evaluate(row, schema, expected_type = expected_type)

class Membership(Expression):
    def __init__(self, col ,args, is_not = False):
        self.col = col
        self.args = tuple(args)
        self.argset = set(arg.evaluate({}, {}) for arg in args
                          if isinstance(arg, LiteralExpression)) 
        self.is_not = is_not
        
    def evaluate(self, row, schema):
        value = self.col.evaluate(row, schema)
        if self.argset:
            result = value in self.argset
        else:
            result = value in [arg.evaluate(row, schema) for arg in self.args]
        return not result if self.is_not else result

class NegationCondition(Expression):
    def __init__(self, expression):
        self.expression = expression
    
    def evaluate(self, row, schema):
        
        return not(self.expression.evaluate(row, schema))

class IsNullCondition(Expression):
    def __init__(self, expression, is_null = True):
        self.is_null = is_null
        self.expression = expression
        
    def evaluate(self, row, schema):
        if self.is_null:
            return self.expression.evaluate(row, schema) is None
        return self.expression.evaluate(row, schema) is not None
        

class LikeCondition(Expression):
    def __init__(self, expression, pattern_expression, is_not=False):
        self.expression = expression
        self.pattern_expression = pattern_expression
        self.is_not = is_not
        # Pre-compile the regex pattern for efficiency (if pattern is constant)
        # Note: We can't compile here if pattern_expression is column reference
        self._compiled_regex = None

    def _pattern_to_regex(self, pattern):
        if not isinstance(pattern, str):
            raise ValueError("LIKE pattern must be a string")
        
        regex_pattern = []
        for char in pattern:
            if char == '%':
                regex_pattern.append('.*')
            elif char == '_':
                regex_pattern.append('.')
            elif char in '.^$*+?{}[]\\|()':
                regex_pattern.append('\\' + char)
            else:
                regex_pattern.append(char)
        
        return '^' + ''.join(regex_pattern) + '$'

    def evaluate(self, row, schema):
        # Properly handle both list and dict cases
        if isinstance(row, list):
            if not row:
                return False
            # Use the first row for evaluation
            current_value = self.expression.evaluate(row[0], schema)
            pattern_value = self.pattern_expression.evaluate(row[0], schema)
        else:
            # Single row case
            current_value = self.expression.evaluate(row, schema)
            pattern_value = self.pattern_expression.evaluate(row, schema)
        
        # Handle NULL values
        if current_value is None or pattern_value is None:
            return False
        
        if not isinstance(pattern_value, str):
            raise ValueError("LIKE pattern must be a string")
        
        # Convert pattern to regex
        regex_pattern = self._pattern_to_regex(pattern_value)
        
        # Compile regex (with cache for efficiency if needed)
        if self._compiled_regex is None or self._compiled_regex.pattern != regex_pattern:
            self._compiled_regex = re.compile(regex_pattern)
        
        # Perform the match and apply NOT if needed
        match_result = self._compiled_regex.match(str(current_value)) is not None
        return not match_result if self.is_not else match_result
    
class OrderBy(Expression):
    def __init__(self, expression, direction = "ASC"):
        self.expression = expression
        self.direction = direction
        

class MathFunction(Expression):
    def __init__(self, name,  expression, round_by = None, alias = None):
        self.name = name
        self.expression = expression
        self.round_by = round_by
        self.alias = alias
        
    def evaluate(self, row, schema):
        value = self.expression.evaluate(row, schema)
        if value is None:
            return value
        elif type(value) not in (float, int):
            raise ValueError('math functions work only with numeric values')
        if self.name == "ROUND":
            if self.round_by:
                return round(value, self.round_by)
            return round(value)
        elif self.name == "CEIL":
            return math.ceil(value)
        elif self.name == "FLOOR":
            return math.floor(value)
        elif self.name == "ABS":
            return abs(value)
        
class StringFunction(Expression):
    def __init__(self, name, expression, start = None, length = None, alias = None):
        self.name = name
        self.expression = expression
        self.start =  start
        self.length = length
        self.alias = alias
        
    def evaluate(self, row, schema):
        value = self.expression.evaluate(row, schema)
        if type(value) != str:
            raise ValueError("string functions work only with strings")
        
        if self.name == "UPPER":
            return value.upper()
        elif self.name == "LOWER":
            return value.lower()
        elif self.name == "LENGTH":
            return len(value)
        elif self.name == "SUBSTRING":
            if self.start:
                value = value[self.start:]
            if self.length:
                value = value[:self.length]
            return value
        elif self.name == "REVERSE":
            return value[::-1]
            

class Replace(Expression):
    def __init__(self, expression, old, new, name = "REPLACE", alias = None):
        self.name = name
        self.expression = expression
        self.old = old
        self.new = new
        self.alias = alias
        
        
    def evaluate(self, row, schema):
        value = self.expression.evaluate(row, schema)
        old = self.old.evaluate(row, schema)
        new = self.new.evaluate(row, schema)
        if type(value) != str or type(old) != str or type(new) != str:
            raise ValueError("string functions work only with strings") 
        if old not in str(value):
            return value
        val = value.replace(old, new)
        return val


class Concat(Expression):
    def __init__(self, expressions, name = "Concat", alias = None):
        self.expressions = expressions
        self.name = name
        self.alias = alias
        
    def evaluate(self, row, schema):
        res = ""
        for exp in self.expressions:
            val = exp.evaluate(row, schema)
            if val is not None:
                res += str(val)
            
        return res if res else None
    
class Cast(Expression):
    def __init__(self, expression, target_type, name = "Cast", alias = None):
        self.expression = expression
        self.target_type = target_type
        self.name = name
        self.alias = alias
        
    def evaluate(self, row, schema):
        value = self.expression.evaluate(row, schema)
        if value is None:
            return None
        
        if self.target_type in ["INT", "INTEGER"]:
            if type(value) not in (float, int):
                raise ValueError (f"Given Expression has datatype of {type(value).__name__} but INT were Given")
            return int(value)
            
        elif self.target_type in ['VARCHAR', "STRING", "TEXT"]:
            return str(value)
        elif self.target_type in ['FLOAT', "DECIMAL"]:
            if type(value) not in (float, int):
                raise ValueError (f"Given Expression has datatype of {type(value).__name__} but {self.target_type} target type  were Given")
            return float(value)
        elif self.target_type in ["DATE"]:
            if type(value) != str:
                raise ValueError (f"Given Expression has datatype of {type(value).__name__} but {self.target_type} target type  were Given")
            try:
                return datetime.strptime(value, "%Y-%m-%d").date()
            except ValueError:
                raise ValueError(f"Invalid DATE format: {value}. Expected YYYY-MM-DD.")
        elif self.target_type in ["TIME"]:
            if type(value) != str:
                raise ValueError (f"Given Expression has datatype of {type(value).__name__} but {self.target_type} target type  were Given")
            try:
                return datetime.strptime(value, "%H:%M:%S").time()
            except ValueError:
                raise ValueError(f"Cannot convert '{value}' to TIME. Format must be HH:MM:SS")
        

class CoalesceFunction(Expression):
    def __init__(self, expressions, name = "COALESCE", alias = None):
        self.expressions = expressions
        self.name = name
        self.alias = alias
    def evaluate(self, row, schema):
        for exr in self.expressions:
            val = exr.evaluate(row, schema)
            if val is not None:
                return val
        return None
            
        
class NullIF(Expression):
    def __init__(self, expression, number, name = "NULLIF", alias = None):
        self.expression = expression
        self.number = number
        self.name = name
        self.alias = alias
        
        
    def evaluate(self, row, schema):
        a = self.expression.evaluate(row, schema)
        b = self.number.evaluate(row, schema)
        if type(a) in (float, int):
            if type(b) not in (float, int):
                raise ValueError("NULLIF must take arguments with the same datatype")
        if type(a) == str:
            if type(b) != str:
                raise ValueError("NULLIF must take arguments with the same datatype")
                
        
        if a == b:
            return None
        return a

class CurrentDate(Expression):
    def __init__(self, name = "CURRENT_DATE", alias = None):
        self.name = name
        self.alias = alias
        
    def evaluate(self, row = None, schema= None):
        return datetime.date.today()
    

class NowFunction(Expression):
    def __init__(self, name = "NOW", alias = None):
        self.name = name
        self.alias = alias
        
        
    def evaluate(self, row = None, schema =None):
        return datetime.datetime.now()
    
class Extract(Expression):
    def __init__(self, expression, part, name = "EXTRACT", alias = None):
        self.expression = expression
        self.part = part
        self.name = name
        self.alias = alias
        
    def evaluate(self, row, schema):
        # Handle both single row and aggregate result cases
        if isinstance(self.expression, Function):
            # If the inner expression is an aggregate function, 
            # it needs to be evaluated differently
            if isinstance(row, list):
                # This is a group of rows for aggregate evaluation
                value = self.expression.evaluate(row, schema)
            else:
                # This is a single row, but we need to pass it as a list to the function
                value = self.expression.evaluate([row], schema)
        else:
            # Regular expression evaluation with single row
            if isinstance(row, list):
                if not row:
                    return None
                # Use first row if we have a list
                value = self.expression.evaluate(row[0], schema)
            else:
                value = self.expression.evaluate(row, schema)
        
        if value is None:
            return None
            
        # Import here to avoid circular imports
        from datetime import date, time
        
        if not isinstance(value, (date, time)):
            raise ValueError("EXTRACT Function works Only with DATE & TIME columns")
        
        if self.part == 'YEAR':
            return value.year
        elif self.part == 'MONTH':
            return value.month
        elif self.part == 'DAY':
            return value.day
        elif self.part == 'HOUR':
            return value.hour
        elif self.part == 'MINUTE':
            return value.minute
        elif self.part == 'SECOND':
            return value.second
class DateDIFF(Expression):
    def __init__(self, date1, date2, unit = 'days'):
        self.date1 = date1
        self.date2 = date2
        self.unit = unit
        
            
