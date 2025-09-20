from errors import *
from datatypes import *
import math
import re

def get_execute_function():
    from utilities import execute
    return execute

def get_db_manager():
    from utilities import db_manager
    return db_manager




class SelectStatement:
    
    __slots__ = ['columns', 'function_columns', 'table', 'where', 'distinct', 
                 'order_by', 'group_by', 'having', 'offset', 'limit']
    
    def __init__(self, columns = None, function_columns = None, table = None, where = None, distinct = False, order_by = None, group_by = None, having = None, offset = None, limit = None, name = "SELECT_STATEMENT"):
        
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
        
        
        
    def evaluate(self, row = None, schema = None, runner_context=None):
            execute_fn = get_execute_function()
            db_mgr = get_db_manager()
        
            result = execute_fn(self, db_mgr.active_db)

            return result
    
class InsertExpression():
    def __init__(self, values, columns = None):
        self.columns = columns or []
        self.values = values
        
    

        
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
    def __init__(self, table, insertion_data, conflict = False, conflict_targets = None, action = None, update_cols = None, returned_cols = None):
        self.table = table
        self.insertion_data = insertion_data
        self.conflict = conflict
        self.conflict_targets = conflict_targets
        self.action = action
        self.update_cols = update_cols
        self.returned_cols = returned_cols
        
class UpdateStatement:
    def __init__(self, table, columns, where = None, returned_columns = None):
        self.table = table
        self.columns = columns
        self.where = where
        self.returned_columns = returned_columns

class DeleteStatement:
    def __init__(self, table, where = None, returned_columns = None):
        self.table = table
        self.where = where
        self.returned_columns = returned_columns
        
class CreateDatabseStatement:
    def __init__(self, database_name):
        self.database_name = database_name
        
class CreateTableStatement:
    def __init__(self, table_name, schema, defaults, auto, constraints, restrictions, private_constraints, constraints_ptr):
        self.table_name = table_name
        self.schema = schema
        self.defaults = defaults
        self.auto = auto
        self.constraints = constraints
        self.restrictions = restrictions
        self.private_constraints = private_constraints
        self.constraints_ptr = constraints_ptr

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
            raise TypeError(f"ColumnExpression.evaluate expects a dictionary (row), got {type(row)}")
            
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
    def __init__(self, value, alias = None, name = "LiteralExpression"):
        self.value = value
        self.alias = alias
        self.name = name
    
    def evaluate(self, row, schema, expected_type = None):
        
        if expected_type is not None and issubclass(expected_type, SQLType):
            converted_value = expected_type(self.value).value            
            return converted_value
        return self.value
    
    def get_referenced_columns(self):
        return set()

class BinaryOperation(Expression):
    def __init__(self, left, operator, right, alias = None, name = "Operation"):
        self.left = left        
        self.operator = operator 
        self.right = right      
        self.alias = alias
        self.name = name
    
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
    def __init__(self, left, operator, right, context = None):
        self.left = left
        self.operator = operator
        self.right = right
        self.context = context
        

    def evaluate(self, row_or_rows, schema):
        if self.context == "HAVING":
            # For HAVING, we need to handle aggregates vs regular columns differently
            left = self._evaluate_having_expr(self.left, row_or_rows, schema)
            right = self._evaluate_having_expr(self.right, row_or_rows, schema)
        elif self.context == "WHERE":
            # For WHERE, evaluate normally with single row
            left = self._evaluate_where_expr(self.left, row_or_rows, schema)
            right = self._evaluate_where_expr(self.right, row_or_rows, schema)
        else:
            left = self.left.evaluate(row_or_rows, schema)
            right = self.right.evaluate(row_or_rows, schema)

        # Debug print to see what values we're getting

        # Handle NULL values first
        if left is None or right is None:
            if self.operator in ["AND", "OR"]:
                # For logical operators with NULL
                if self.operator == "AND":
                    return False if (left is False or right is False) else None
                else:  # OR
                    return True if (left is True or right is True) else None
            else:
                # For comparison operators with NULL
                return False
        
        # Convert string comparisons to lowercase for case-insensitive comparison
        if isinstance(right, str) and isinstance(left, str):
            left = left.lower()
            right = right.lower()
        
        # Comparison operations - MUST return boolean values
        if self.operator == '=': 
            result = left == right
        elif self.operator == ">": 
            result = left > right
        elif self.operator == "<": 
            result = left < right
        elif self.operator == "<=": 
            result = left <= right
        elif self.operator == ">=": 
            result = left >= right
        elif self.operator == "!=": 
            result = left != right
        elif self.operator == "AND": 
            # Ensure boolean conversion
            result = bool(left) and bool(right)
        elif self.operator == "OR": 
            # Ensure boolean conversion
            result = bool(left) or bool(right)
        else:
            raise ValueError(f"Unknown operator: {self.operator}")
        
        
        return result
    
    def _evaluate_having_expr(self, expr, group_rows, schema):
        
        
        
        if isinstance(expr, Function):
            # Aggregate functions need the full group
            result = expr.evaluate(group_rows, schema)
            
            return result
            
        elif isinstance(expr, Cast):
            # CAST expressions - evaluate the inner expression first
            inner_value = self._evaluate_having_expr(expr.expression, group_rows, schema)
            
            # Now apply the cast to the result
            if inner_value is None:
                return None
            
            if expr.target_type in ["INT", "INTEGER"]:
                if type(inner_value) not in (float, int):
                    raise ValueError(f"Given Expression has datatype of {type(inner_value).__name__} but INT were Given")
                return int(inner_value)
                
            elif expr.target_type in ['VARCHAR', "STRING", "TEXT"]:
                return str(inner_value)
            elif expr.target_type in ['FLOAT', "DECIMAL"]:
                if type(inner_value) not in (float, int):
                    raise ValueError(f"Given Expression has datatype of {type(inner_value).__name__} but {expr.target_type} target type  were Given")
                return float(inner_value)
            # Add other cast types as needed...
            
        elif isinstance(expr, Extract):
            # Extract expressions - evaluate the inner expression first
            inner_value = self._evaluate_having_expr(expr.expression, group_rows, schema)
            
            if inner_value is None:
                return None
                
            from datetime import date, time
            if not isinstance(inner_value, (date, time)):
                raise ValueError("EXTRACT Function works Only with DATE & TIME columns")
            
            if expr.part == 'YEAR':
                return inner_value.year
            elif expr.part == 'MONTH':
                return inner_value.month
            elif expr.part == 'DAY':
                return inner_value.day
            elif expr.part == 'HOUR':
                return inner_value.hour
            elif expr.part == 'MINUTE':
                return inner_value.minute
            elif expr.part == 'SECOND':
                return inner_value.second
                
        elif isinstance(expr, ColumnExpression):
            # Regular columns: use value from first row (all rows in group have same GROUP BY values)
            if group_rows:
                result = expr.evaluate(group_rows[0], schema)
                
                return result
            else:
                return None
                
        elif isinstance(expr, LiteralExpression):
            # Literals evaluate the same regardless of context
            result = expr.evaluate({}, schema)
            
            return result
            
        elif isinstance(expr, BinaryOperation):
            # For binary operations, evaluate each side appropriately
            left = self._evaluate_having_expr(expr.left, group_rows, schema)
            right = self._evaluate_having_expr(expr.right, group_rows, schema)
            
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
            # Recursive case - nested condition
            
            return expr.evaluate(group_rows, schema)
            
        else:
            # For other expression types, try normal evaluation with first row
            
            if group_rows:
                return expr.evaluate(group_rows[0], schema)
            else:
                return None
    
    def _evaluate_where_expr(self, expr, row, schema):
        """Evaluate expression in WHERE context (with single row)"""
        # Handle type coercion for literals
        if isinstance(expr, LiteralExpression):
            # Try to get expected type from the other side of comparison
            other_expr = self.right if expr == self.left else self.left
            if isinstance(other_expr, ColumnExpression):
                expected_type = schema[other_expr.column_name]
                return expr.evaluate(row, schema, expected_type)
        elif isinstance(expr, SelectStatement):
            return expr.evaluate(row, schema)
        
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
        
        if isinstance(self.lower, LiteralExpression):
            lower_value = self.lower.evaluate(row, schema, expected_type)
        else: lower_value = self.lower.evaluate(row, schema)  
        
        if isinstance(self.upper, LiteralExpression):
            upper_value = self.upper.evaluate(row, schema, expected_type)
        else: upper_value = self.upper.evaluate(row, schema)
        
        expr_value = self.expression.evaluate(row, schema)
        
        if expr_value is None or lower_value is None or upper_value is None:
            return False
        if not self.is_not:
            return lower_value<=self.expression.evaluate(row, schema)<=upper_value
        return not lower_value<=self.expression.evaluate(row, schema)<=upper_value

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
        self._compiled_regex = None
        self._cached_pattern = None

    def __setstate__(self, state):
        """Called after deserialization to reset non-serializable attributes"""
        # Reset regex-related attributes after deserialization
        self._compiled_regex = None
        self._cached_pattern = None

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
        
        # Cache compiled regex for efficiency, but handle deserialization issues
        if (self._compiled_regex is None or 
            self._cached_pattern != regex_pattern or 
            not hasattr(self._compiled_regex, 'pattern')):
            self._compiled_regex = re.compile(regex_pattern)
            self._cached_pattern = regex_pattern
        
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
        # Handle the inner expression based on its type
        if isinstance(self.expression, Function):
            # Aggregate functions need the full list
            if isinstance(row, list):
                value = self.expression.evaluate(row, schema)
            else:
                value = self.expression.evaluate([row], schema)
        elif isinstance(self.expression, ColumnExpression):
            # Column expressions need a single row
            if isinstance(row, list):
                value = self.expression.evaluate(row[0], schema) if row else None
            else:
                value = self.expression.evaluate(row, schema)
        else:
            # For other expressions
            if isinstance(row, list):
                value = self.expression.evaluate(row[0], schema) if row else None
            else:
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
            import math
            return math.ceil(value)
        elif self.name == "FLOOR":
            import math
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
            # Handle different expression types appropriately
            if isinstance(exp, Function):
                # Aggregate functions need the full list
                if isinstance(row, list):
                    val = exp.evaluate(row, schema)
                else:
                    val = exp.evaluate([row], schema)
            elif isinstance(exp, ColumnExpression):
                # Column expressions need a single row dict
                if isinstance(row, list):
                    # In GROUP BY context, all rows in group have same GROUP BY column values
                    val = exp.evaluate(row[0], schema) if row else None
                else:
                    val = exp.evaluate(row, schema)
            elif isinstance(exp, (Cast, Extract, MathFunction, StringFunction)):
                # Wrapper expressions - let them handle the context
                val = exp.evaluate(row, schema)
            else:
                # For other expressions (literals, etc.)
                if isinstance(row, list):
                    val = exp.evaluate(row[0], schema) if row else None
                else:
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
        # Handle the inner expression based on its type
        if isinstance(self.expression, Function):
            # Aggregate functions need the full list
            if isinstance(row, list):
                value = self.expression.evaluate(row, schema)
            else:
                value = self.expression.evaluate([row], schema)
        elif isinstance(self.expression, ColumnExpression):
            # Column expressions need a single row
            if isinstance(row, list):
                value = self.expression.evaluate(row[0], schema) if row else None
            else:
                value = self.expression.evaluate(row, schema)
        elif isinstance(self.expression, (MathFunction, StringFunction)):
            # Let wrapper functions handle their own context
            value = self.expression.evaluate(row, schema)
        else:
            # For literals and other simple expressions
            if isinstance(row, list):
                value = self.expression.evaluate(row[0], schema) if row else None
            else:
                value = self.expression.evaluate(row, schema)
        
        if value is None:
            return None
        
        # Apply the cast
        if self.target_type in ["INT", "INTEGER"]:
            if type(value) not in (float, int):
                raise ValueError(f"Given Expression has datatype of {type(value).__name__} but INT were Given")
            return int(value)
            
        elif self.target_type in ['VARCHAR', "STRING", "TEXT"]:
            return str(value)
        elif self.target_type in ['FLOAT', "DECIMAL"]:
            if type(value) not in (float, int):
                raise ValueError(f"Given Expression has datatype of {type(value).__name__} but {self.target_type} target type  were Given")
            return float(value)
        elif self.target_type in ["DATE"]:
            if type(value) != str:
                raise ValueError(f"Given Expression has datatype of {type(value).__name__} but {self.target_type} target type  were Given")
            try:
                from datetime import datetime
                return datetime.strptime(value, "%Y-%m-%d").date()
            except ValueError:
                raise ValueError(f"Invalid DATE format: {value}. Expected YYYY-MM-DD.")
        elif self.target_type in ["TIME"]:
            if type(value) != str:
                raise ValueError(f"Given Expression has datatype of {type(value).__name__} but {self.target_type} target type  were Given")
            try:
                from datetime import datetime
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
        return date.today()

    

class NowFunction(Expression):
    def __init__(self, name = "NOW", alias = None):
        self.name = name
        self.alias = alias
        
    def evaluate(row = None, schema = None):
        return datetime.now()
    
class CurrentTime(Expression):
    def __init__(self, name = "CURRENT_TIME", alias = None):
        self.name = name
        self.alias = alias
        
    def evaluate(row = None, schema = None):
        return datetime.now().time().strftime("%H:%M:%S")


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
    def __init__(self, date1, date2, unit = 'days', name = "DATEDIFF", alias = None):
        self.date1 = date1
        self.date2 = date2
        self.unit = unit  # This can be a LiteralExpression
        self.name = name
        self.alias = alias
        
    def evaluate(self, row, schema):
        # Evaluate dates
        if isinstance(self.date1, CurrentDate):
            date1_val = self.date1.evaluate(row, schema)
        else:
            date1_val = self.date1.evaluate(row, schema)
            
        if isinstance(self.date2, ColumnExpression):
            date2_val = self.date2.evaluate(row, schema)
        else:
            date2_val = self.date2.evaluate(row, schema)
        
        # Get unit - it might be a LiteralExpression
        if isinstance(self.unit, LiteralExpression):
            unit_val = self.unit.evaluate(row, schema)
        elif isinstance(self.unit, str):
            unit_val = self.unit
        else:
            unit_val = self.unit.evaluate(row, schema)
        
        # Calculate difference
        if hasattr(date1_val, 'year') and hasattr(date2_val, 'year'):
            # Both are dates
            years = date1_val.year - date2_val.year
            months = date1_val.month - date2_val.month
            days = (date1_val - date2_val).days
            
            if unit_val.lower() == 'years':
                return years
            elif unit_val.lower() == 'months':
                return years * 12 + months  # Approximate
            elif unit_val.lower() == 'days':
                return days
            else:
                return days  # default to days
        else:
            raise ValueError("DATEDIFF requires date arguments")

        
        
class CaseWhen(Expression):
    def __init__(self, expressions, actions, case_else = None, name = "CASE", alias = None):
        self.expressions = expressions
        self.actions = actions
        self.case_else = case_else
        self.name = name
        self.alias = alias
        
    def evaluate(self, row, schema):
        # Handle GROUP BY context where row might be a list
        if isinstance(row, list):
            # In GROUP BY context, use first row since GROUP BY columns have same values
            eval_row = row[0] if row else {}
        else:
            eval_row = row
        
        for i, expr in enumerate(self.expressions):
            if expr.evaluate(eval_row, schema):
                return self.actions[i].evaluate(eval_row, schema)
        
        if self.case_else:
            return self.case_else.evaluate(eval_row, schema)
        
        return None
    
class TableReference(Expression):
    def __init__(self, table_name, alias=None):
        self.table_name = table_name
        self.alias = alias  # 'e' for 'employees as e'
    
    def evaluate(self):
        return self.table_name
        
        
class QualifiedColumnExpression(ColumnExpression):
    def __init__(self, table_name, column_name, alias=None):
        self.table_name = table_name
        self.column_name = column_name
        self.alias = alias
        
def evaluate(self, row, schema):
        # For now, just use column_name (single table)
        # Later: resolve from specific table in joins
        return super().evaluate(row, schema)
    
    
class Exists(Expression):
    def __init__(self, subquery, name = "EXISTS", alias  = None):
        self.subquery = subquery
        self.name = name
        self.alias = alias
        
    def evaluate(self, row, schema):
        
        res = self.subquery.evaluate(row, schema)
        if res: return True
        return False
        

class ShowConstraints(Expression):
    def __init__(self, table_name, col = None ,name = "REQUEST_CONSTRAINTS", alias = None):
        self.table_name = table_name
        self.name = name
        self.alias = alias
        self.col = col
        
    def evaluate(self):
        database = get_db_manager().active_db
        if self.table_name not in database:
            raise TableNotFoundError(self.table_name)
        else:
            if not database[self.table_name].private_constraints:
                raise ValueError('Your table has no constraints, use ALTER <table_name> ADD <constraint>* <column_name>')
            private_constraints = database[self.table_name].private_constraints
            if not self.col:
                return [private_constraints]
            else:
                row = {}
                for c, v in  private_constraints.items():
                    if self.col == c:
                       row[self.col] = v
                       return [row]
            return None
                    
            
class UnionExpression(Expression):
    def __init__(self, left, right, context="UNION"):
            self.left = left
            self.right = right
            self.context = context
            
    def evaluate(self, row=None, schema=None):
        """
        Evaluate UNION - combines results from two SELECT statements
        and removes duplicates (unlike UNION ALL)
        """
        # Get results from both SELECT statements
        left_result = self.left.evaluate(row, schema) if hasattr(self.left, 'evaluate') else self.left
        right_result = self.right.evaluate(row, schema) if hasattr(self.right, 'evaluate') else self.right
        
        # Ensure we have lists of dictionaries
        if not isinstance(left_result, list):
            left_result = [left_result] if left_result else []
        if not isinstance(right_result, list):
            right_result = [right_result] if right_result else []
            
        # Get column names from left table (UNION uses left table's column names)
        if not left_result:
            return right_result  # If left is empty, return right
        if not right_result:
            return left_result   # If right is empty, return left
            
        left_columns = list(left_result[0].keys())
        
        # Validate that both results have the same number of columns
        if right_result and len(left_columns) != len(list(right_result[0].keys())):
            raise ValueError(f"UNION requires same number of columns. "
                        f"Left has {len(left_columns)}, right has {len(list(right_result[0].keys()))}")
        
        # Align right table columns to match left table column names
        aligned_right_result = []
        for row in right_result:
            right_values = list(row.values())
            aligned_row = {left_columns[i]: right_values[i] for i in range(len(left_columns))}
            aligned_right_result.append(aligned_row)
        
        combined_result = left_result + aligned_right_result
        
        # Remove duplicates for UNION (keep for UNION ALL)
        if self.context == "UNION":
            unique_result = []
            seen = set()
            
            for row in combined_result:
                
                row_tuple = tuple(sorted(row.items()))
                
                if row_tuple not in seen:
                    seen.add(row_tuple)
                    unique_result.append(row)
            
            return unique_result
        else:  # UNION ALL
            return combined_result
        
                
                
class IntersectExpression(Expression):
    def __init__(self, left, right, name="INTERSECT", alias=None):
        self.left = left
        self.right = right
        self.name = name
        self.alias = alias

    def evaluate(self, row=None, schema=None):
        # Evaluate left and right expressions
        left_result = self.left.evaluate(row, schema) if hasattr(self.left, "evaluate") else self.left
        right_result = self.right.evaluate(row, schema) if hasattr(self.right, "evaluate") else self.right

        # Normalize to lists of dicts
        if not isinstance(left_result, list):
            left_result = [left_result] if left_result else []
        if not isinstance(right_result, list):
            right_result = [right_result] if right_result else []

        if not left_result or not right_result:
            return []

        # Ensure both sides have same number of columns
        left_columns = list(left_result[0].keys())
        right_columns = list(right_result[0].keys())
        if len(left_columns) != len(right_columns):
            raise ValueError(
                f"INTERSECT requires same number of columns. "
                f"Left has {len(left_columns)}, right has {len(right_columns)}"
            )

        aligned_right_result = []
        for row in right_result:
            right_values = list(row.values())
            aligned_row = {left_columns[i]: right_values[i] for i in range(len(left_columns))}
            aligned_right_result.append(aligned_row)

        
        left_set = {tuple(row.items()) for row in left_result}
        right_set = {tuple(row.items()) for row in aligned_right_result}

        intersected = left_set & right_set
        
        return [dict(row) for row in intersected]
    
    
   
class ExceptExpression(Expression):
    def __init__(self, left, right, name="EXCEPT", alias=None):
        self.left = left
        self.right = right
        self.name = name
        self.alias = alias
        
    def evaluate(self, row=None, schema=None):
        # Evaluate left and right expressions
        left_result = self.left.evaluate(row, schema) if hasattr(self.left, "evaluate") else self.left
        right_result = self.right.evaluate(row, schema) if hasattr(self.right, "evaluate") else self.right

        # Normalize to lists of dicts
        if not isinstance(left_result, list):
            left_result = [left_result] if left_result else []
        if not isinstance(right_result, list):
            right_result = [right_result] if right_result else []

        if not left_result or not right_result:
            return []

        # Ensure both sides have same number of columns
        left_columns = list(left_result[0].keys())
        right_columns = list(right_result[0].keys())
        if len(left_columns) != len(right_columns):
            raise ValueError(
                f"EXCEPT requires same number of columns. "
                f"Left has {len(left_columns)}, right has {len(right_columns)}"
            )

        aligned_right_result = []
        for row in right_result:
            right_values = list(row.values())
            aligned_row = {left_columns[i]: right_values[i] for i in range(len(left_columns))}
            aligned_right_result.append(aligned_row)

        
        left_set = {tuple(row.items()) for row in left_result}
        right_set = {tuple(row.items()) for row in aligned_right_result}
    
        diff = left_set - right_set
        
        
        return [dict(row) for row in diff]


class ReturningClause:
    def __init__(self, columns , table_name = None):
        self.columns = columns 
        self.table_name = table_name
    
    def evaluate(self, inserted_rows, database):
        """Evaluate RETURNING clause with all inserted row data"""
        if not inserted_rows:
            return []
            
        result_rows = []
        
        for inserted_row in inserted_rows:
            result_row = {}
            
            for column_expr in self.columns:
                if isinstance(column_expr, ColumnExpression):
                    if column_expr.column_name == "*":
                        # Return all columns from this row
                        for col, value in inserted_row.items():
                            if isinstance(value, SQLType):
                                result_row[col] = value.value
                            else:
                                result_row[col] = value
                    else:
                        # Return specific column
                        col_name = column_expr.column_name
                        if col_name in inserted_row:
                            value = inserted_row[col_name]
                            if isinstance(value, SQLType):
                                result_row[col_name] = value.value
                            else:
                                result_row[col_name] = value
                # Handle other expression types (literals, functions, etc.)
                elif isinstance(column_expr, LiteralExpression):
                    alias = getattr(column_expr, 'alias', 'literal')
                    result_row[alias] = column_expr.evaluate({}, {})
                        
            result_rows.append(result_row)  
        return result_rows