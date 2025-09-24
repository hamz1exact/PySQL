from errors import *
from datatypes import *
import math
import re
from database_manager import Table
from constants import *

def get_execute_function():
    from executor import execute
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
            result = execute_fn(self, db_mgr)
            if runner_context is None:
                return result
            elif runner_context == "ONE_VALUE":
                if len(result) > 1:
                    raise ValueError ("Subquery must return only one value")
                first_key = list(result[0].keys())[0]
                return result[0][first_key]
            else:
                if len(result)>=1 and len(result[0]) > 1:
                    raise ValueError("Subquery must return only one value")
                else:
                    return result if len(result)>=1 else []
                
    
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
    def __init__(self, table_name, schema, defaults = None, auto = None, constraints = None, restrictions = None, private_constraints = None, constraints_ptr = None):
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
    def __init__(self, left, operator, right, context=None):
        self.left = left
        self.operator = operator
        self.right = right
        self.context = context

    def evaluate(self, row_or_rows, schema):
        if self.context == "HAVING":
            left = self._evaluate_having_expr(self.left, row_or_rows, schema)
            right = self._evaluate_having_expr(self.right, row_or_rows, schema)
        elif self.context == "WHERE":
            left = self._evaluate_where_expr(self.left, row_or_rows, schema)
            right = self._evaluate_where_expr(self.right, row_or_rows, schema)
        else:
            # Handle subqueries in any context
            if isinstance(self.left, SelectStatement):
                left = self.left.evaluate(row_or_rows, schema, runner_context="ONE_VALUE")
            else:
                left = self.left.evaluate(row_or_rows, schema)
                
            if isinstance(self.right, SelectStatement):
                right = self.right.evaluate(row_or_rows, schema, runner_context="ONE_VALUE")
            else:
                right = self.right.evaluate(row_or_rows, schema)

        # Handle NULL values first
        if left is None or right is None:
            if self.operator in ["AND", "OR"]:
                if self.operator == "AND":
                    return False if (left is False or right is False) else None
                else:  # OR
                    return True if (left is True or right is True) else None
            else:
                return False
        
        # Convert string comparisons to lowercase for case-insensitive comparison
        if isinstance(right, str) and isinstance(left, str):
            left = left.lower()
            right = right.lower()
        
        # Apply operator
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
            result = bool(left) and bool(right)
        elif self.operator == "OR": 
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
            exp = expr.evaluate(row, schema, runner_context="ONE_VALUE")
            return exp
                
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

        # --- Lower bound ---
        if isinstance(self.lower, SelectStatement):
            lower_value = self.lower.evaluate(row, schema, runner_context="ONE_VALUE")
        elif isinstance(self.lower, LiteralExpression):
            lower_value = self.lower.evaluate(row, schema, expected_type)
        else:
            lower_value = self.lower.evaluate(row, schema)

        # --- Upper bound ---
        if isinstance(self.upper, SelectStatement):
            upper_value = self.upper.evaluate(row, schema, runner_context="ONE_VALUE")
        elif isinstance(self.upper, LiteralExpression):
            upper_value = self.upper.evaluate(row, schema, expected_type)
        else:
            upper_value = self.upper.evaluate(row, schema)

        # --- Expression value ---
        expr_value = self.expression.evaluate(row, schema)

        if expr_value is None or lower_value is None or upper_value is None:
            return False

        if not self.is_not:
            return lower_value <= expr_value <= upper_value
        return not (lower_value <= expr_value <= upper_value)

class Membership(Expression):
    def __init__(self, col ,args, is_not = False):
        self.col = col
        self.args = tuple(args)
        self.argset = set(arg.evaluate({}, {}) for arg in args
                          if isinstance(arg, LiteralExpression)) 
        self.is_not = is_not
        
    def evaluate(self, row, schema):
        value = self.col.evaluate(row, schema)
        out = []
        if self.argset:
            result = value in self.argset
        else:
            for arg in self.args:
                if isinstance(arg, SelectStatement):
                    
                    exp = arg.evaluate(row, schema, runner_context="ANY")
                    seen = set()
                    for row in exp:
                        for v in row.values():
                            if v not in seen:
                                seen.add(v)
                                out.append(v)
                else:
                    out.append(arg.evaluate(row, schema))
            result = value in out
            
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
        if isinstance(self.expression, SelectStatement):
            expr = self.expression.evaluate(row, schema, runner_context="ONE_VALUE")
        else:
            expr = self.expression.evaluate(row, schema)
        if self.is_null:
            return expr is None
        return expr is not None
        

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
            try:
                # Handle different expression types in WHEN conditions
                if isinstance(expr, ConditionExpr):
                    # ConditionExpr can handle subqueries internally
                    condition_result = expr.evaluate(eval_row, schema)
                elif isinstance(expr, SelectStatement):
                    # Direct subquery in WHEN clause
                    subquery_result = expr.evaluate(eval_row, schema, runner_context="ONE_VALUE")
                    condition_result = bool(subquery_result)
                else:
                    # Regular expressions
                    condition_result = expr.evaluate(eval_row, schema)
                
                if condition_result:
                    # Evaluate the THEN action
                    action = self.actions[i]
                    if isinstance(action, SelectStatement):
                        # Subquery in THEN clause
                        return action.evaluate(eval_row, schema, runner_context="ONE_VALUE")
                    else:
                        return action.evaluate(eval_row, schema)
                        
            except Exception as e:
                # If evaluation fails, continue to next condition
                print(f"Warning: Error evaluating CASE condition {i}: {e}")
                continue
        
        # No conditions matched, evaluate ELSE clause
        if self.case_else:
            if isinstance(self.case_else, SelectStatement):
                # Subquery in ELSE clause
                return self.case_else.evaluate(eval_row, schema, runner_context="ONE_VALUE")
            else:
                return self.case_else.evaluate(eval_row, schema)
        
        return None
    
class TableReference(Expression):
    def __init__(self, table_name, alias=None):
        self.table_name = table_name  # Can be string or SelectStatement
        self.alias = alias
    
    def evaluate(self):
        if isinstance(self.table_name, SelectStatement):
            # This is a subquery in FROM clause
            return self.table_name
        return self.table_name
        
        
class QualifiedColumnExpression(ColumnExpression):
    def __init__(self, table_name, column_name, alias=None):
        self.table_name = table_name
        self.column_name = column_name
        self.alias = alias
        
    def evaluate(self, row, schema):
        # For qualified columns like f1.account_holder
        # In the current single-table context, just use column_name
        # TODO: Enhance for multi-table joins
        if isinstance(row, dict) and self.column_name in row:
            value = row[self.column_name]
            if isinstance(value, SQLType):
                return value.value
            return value
        elif isinstance(row, dict) and self.column_name == "*":
            return row
        else:
            raise KeyError(f"Column '{self.table_name}.{self.column_name}' not found")
    
    def get_referenced_columns(self):
        return {self.column_name}
    
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
    def __init__(self, table_name, col = None, name = "REQUEST_CONSTRAINTS", alias = None, names = False):
        self.table_name = table_name
        self.name = name
        self.alias = alias
        self.col = col
        self.names = names
        
    def evaluate(self):
        database = get_db_manager().active_db
        if self.table_name not in database:
            raise TableNotFoundError(self.table_name)
        else:
            if not database[self.table_name].private_constraints:
                raise ValueError('Your table has no constraints, use ALTER <table_name> ADD <constraint>* <column_name>')
            private_constraints = database[self.table_name].private_constraints
            constraints_ptr = database[self.table_name].constraints_ptr
            print(database[self.table_name].constraints)
            print(private_constraints)
            print(constraints_ptr)
            print(database[self.table_name].restrictions)
            if not self.col and self.names:
                print()
                for col, const in private_constraints.items():
                    print(f"{"":5}{col:30}-> {const}\n")
            
            elif not self.col:
                print()
                for col, const in private_constraints.items():
                    more_than_one_const = False
                    constr = ""
                    for c in const:
                        if more_than_one_const:
                            constr += f" and {constraints_ptr[c]}"
                        else:
                            more_than_one_const = True
                            constr += constraints_ptr[c]
                    
                    print(f"{"":5}{col:30}-> {constr}\n")
                    
            else:
                print()
                for c, v in  private_constraints.items():
                    
                    if self.col == c:
                        more_than_one_const = False
                        constr = ""
                        for const in v:
                            if more_than_one_const:
                                constr += f" and {constraints_ptr[const]}"
                        else:
                            more_than_one_const = True
                            constr += constraints_ptr[const]
                        print(f"{c:10}-> {constr}\n") 
                        return
                    
            
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
    
class CreateView:
    def __init__(self, view_name, query, can_be_replaced = False):
        self.view_name = view_name
        self.query = query
        self.can_be_replaced = can_be_replaced
            
class CallView:
    def __init__(self, view_name):
        self.view_name = view_name
        
        
class CTA:
    def __init__(self, table_name, query, with_data = True):
        self.table_name = table_name
        self.query = query
        self.with_data = with_data
    
 
    
class CreateMaterializedView:
    def __init__(self, table_name, query, with_data = True):
        self.table_name = table_name
        self.query = query
        self.with_data = True

class RefreshMaterializedView:
    def __init__(self, mt_view_name):
        self.mt_view_name = mt_view_name
    
    
class DropDatabase:
    def __init__(self, database_name):
        self.database_name = database_name
        
class DropTable:
    def __init__(self, table_name):
        self.table_name = table_name
        
class DropView:
    def __init__(self, view_name):
        self.view_name = view_name

class DropMTView:
    def __init__(self, view_name):
        self.view_name = view_name

class TruncateTable:
    def __init__(self, table_name):
        self.table_name = table_name
        
class WithCTE:
    def __init__(self, cte_expressions, cte_queries):
        self.cte_expressions = cte_expressions
        self.cte_queries = cte_queries
    
    def execute(self, db_manager):
        for cte_table_info in self.cte_expressions:
            cte_rows = self._get_table_rows(cte_table_info.query)
            cte_schema = self._generate_schema(cte_rows)
            post_proccessing = self._create_cte(cte_table_info.cte_name, rows = cte_rows, cte_schema=cte_schema, db_manager=db_manager)
        return self.cte_queries.evaluate()
             
            
            
        
    def _generate_schema(self, rows):
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
    
    def _get_table_rows(self, query):
        return query.evaluate()
    
    def _create_cte(self, cte_name, rows, cte_schema, db_manager):
        new_table = Table(
            name=cte_name, 
            schema=cte_schema, 
            defaults={}, 
            auto={}, 
            constraints={}, 
            restrictions={}, 
            private_constraints={}, 
            constraints_ptr={}
        )
        for row in rows:
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
        db_manager.active_db[cte_name] = new_table
                         
        
class WithCTExpression:
    def __init__(self, cte_name, query):
        self.cte_name = cte_name
        self.query = query
        

class AlterTable:
    def __init__(self, table_name, expressions):
        self.table_name = table_name
        self.expressions = expressions
    
    def execute(self, db_manager):
        if self.table_name not in db_manager.active_db:
            raise TableNotFoundError(self.table_name)
        
        for expr in self.expressions:
            expr.execute(self.table_name, db_manager)
            

class AddColumnFromAlterTable:
    def __init__(self, column_name, datatype, default = None, constraint = None, constraint_rule = None):
        self.column_name = column_name
        self.datatype = datatype
        self.default = default 
        self.constraint = constraint
        self.constraint_rule = constraint_rule
        
    def execute(self, table_name, db_manager):
        
        if self.column_name in db_manager.active_db[table_name].schema:
            raise ValueError("Column Already Exists")
        if self.datatype not in DATATYPE_MAPPING:
            raise ValueError (f"Unknown Data Type {self.datatype}")

        if self.constraint and self.constraint_rule:
            key = f"{table_name}_{self.column_name}_check"
            db_manager.active_db[table_name].private_constraints[self.column_name] = set()
            db_manager.active_db[table_name].private_constraints[self.column_name].add(key)
            db_manager.active_db[table_name].restrictions[self.column_name] = self.constraint_rule
            db_manager.active_db[table_name].constraints_ptr[key] = TokenTypes.CHECK
        elif self.constraint:
            if self.constraint == TokenTypes.NOT_NULL and self.default:
                
                raise ValueError("if a Column has DEFAULT VALUE it cannot inheritance  NOT NULL constraints")
            db_manager.active_db[table_name].constraints[self.column_name] = self.constraint
            constr_id = None
            if self.constraint == TokenTypes.PRIMARY_KEY:
                constr_id = 'pkey'
            elif self.constraint == TokenTypes.NOT_NULL:
                constr_id = '!null'
            elif self.constraint == TokenTypes.UNIQUE:
                constr_id = 'ukey'
            key = f"{table_name}_{self.column_name}_{constr_id}"
            db_manager.active_db[table_name].private_constraints[self.column_name] = set()
            db_manager.active_db[table_name].private_constraints[self.column_name].add(key)
            db_manager.active_db[table_name].constraints_ptr[key] = self.constraint
            
        db_manager.active_db[table_name].schema[self.column_name] = DATATYPE_MAPPING[self.datatype]
        if self.default:
            if self.datatype == TokenTypes.SERIAL:
                raise ValueError('SERIAL Columns Has No Default Value')
            db_manager.active_db[table_name].defaults[self.column_name] = db_manager.active_db[table_name].schema[self.column_name](self.default)
           
        for row in db_manager.active_db[table_name].rows:
            row[self.column_name] = None
            
            

class AddConstraintFromAlterTable:
    def __init__(self, column_name, constraint_name, constraint_type, constraint_rule):
        self.column_name = column_name
        self.constraint_name = constraint_name
        self.constraint_type = constraint_type
        self.constraint_rule = constraint_rule
        
    def execute(self, table_name, db_manager):
        
        if self.column_name not in db_manager.active_db[table_name].schema:
            raise ColumnNotFoundError(self.column_name, table_name=table_name)
        for col, const in db_manager.active_db[table_name].private_constraints.items():
            if col == self.column_name:
                for key in const:
                    if db_manager.active_db[table_name].constraints_ptr[key] == self.constraint_type:
                        raise ValueError(f"Column {self.column_name} already has a {self.constraint_type} Constraint")
        if self.constraint_type and self.constraint_rule:
            if self.column_name in db_manager.active_db[table_name].restrictions:
                raise ValueError(f'{self.column_name} already has a CHECK constraint')
            
            for row in db_manager.active_db[table_name].rows:
                if not self.constraint_rule.evaluate(row, db_manager.active_db[table_name].schema):
                    raise ValueError(
                        f"CHECK constraint '{self.constraint_name}' violated on table '{table_name}'. "
                        f"Exactly at {self.column_name} = {row[self.column_name].value}"
                    )
            if self.column_name not in db_manager.active_db[table_name].private_constraints:
                db_manager.active_db[table_name].private_constraints[self.column_name] = set()
            db_manager.active_db[table_name].private_constraints[self.column_name].add(self.constraint_name)
            db_manager.active_db[table_name].constraints_ptr[self.constraint_name] = TokenTypes.CHECK
            db_manager.active_db[table_name].restrictions[self.column_name] = self.constraint_rule
            
        elif self.constraint_type:
            if self.constraint_type in (TokenTypes.PRIMARY_KEY, TokenTypes.NOT_NULL):
                for row in db_manager.active_db[table_name].rows:
                    if row[self.column_name] is None or row[self.column_name].value is None:
                        raise ValueError(
                            f"{self.constraint_type} constraint violated on table '{table_name}', "
                            f"column '{self.column_name}' cannot contain NULL values. "
                            f"Exactly at: {row[self.column_name].value}"
                        )
            if self.constraint_type in (TokenTypes.UNIQUE, TokenTypes.PRIMARY_KEY):
                seen = set()
                for row in db_manager.active_db[table_name].rows:
                    value = row[self.column_name].value
                    if value in seen:
                        raise ValueError(
                            f"Constraint violation on table '{table_name}', column '{self.column_name}': "
                            f"duplicate value '{value}' found for {self.constraint_type.lower()} constraint."
                        )
                    seen.add(value)
                    
            

            if self.column_name not in db_manager.active_db[table_name].private_constraints:
                # Initialize as a set
                db_manager.active_db[table_name].private_constraints[self.column_name] = set()
            else:
                # If it exists but is a list, convert it to a set
                if isinstance(db_manager.active_db[table_name].private_constraints[self.column_name], list):
                    db_manager.active_db[table_name].private_constraints[self.column_name] = set(
                        db_manager.active_db[table_name].private_constraints[self.column_name]
                    )

            # Now it's guaranteed to be a set
            db_manager.active_db[table_name].private_constraints[self.column_name].add(self.constraint_name)

            # Update other mappings
            db_manager.active_db[table_name].constraints_ptr[self.constraint_name] = self.constraint_type
            db_manager.active_db[table_name].constraints[self.column_name] = self.constraint_type
                                

class DropColumnFromAlterTable:
    def __init__(self, column_name):
        self.column_name = column_name
        
    def execute(self, table_name, db_manager):
        if self.column_name not in db_manager.active_db[table_name].schema:
            raise ColumnNotFoundError(column_name=self.column_name,table_name=table_name)
        rows = db_manager.active_db[table_name].rows

        if self.column_name in db_manager.active_db[table_name].private_constraints:
            for col, key_set in db_manager.active_db[table_name].private_constraints.items():
                if col == self.column_name:
                    for key in key_set:
                        if key in db_manager.active_db[table_name].constraints_ptr:
                            del db_manager.active_db[table_name].constraints_ptr[key]
            del db_manager.active_db[table_name].private_constraints[self.column_name]
        if self.column_name in db_manager.active_db[table_name].restrictions:
            del  db_manager.active_db[table_name].restrictions[self.column_name]

        if self.column_name in db_manager.active_db[table_name].constraints:
            del db_manager.active_db[table_name].constraints[self.column_name]
        del db_manager.active_db[table_name].schema[self.column_name]
        for row in rows:
            try:
                del row[self.column_name]
            except Exception as e:
                raise ValueError(e)
        
class DropConstraintFromAlterTable:
    def __init__(self, const_name):
        self.const_name = const_name
    
    def execute(self, table_name, db_manager):
        column_pointer = None
        for col, constr in db_manager.active_db[table_name].private_constraints.items():
            
            if self.const_name in constr:
                column_pointer = col
                for key in constr:
                    if key == self.const_name:
                        print('TRUE')
                        value = db_manager.active_db[table_name].constraints_ptr[key]
                        if value == TokenTypes.CHECK:
                            del db_manager.active_db[table_name].restrictions[col]
                        else:
                            del db_manager.active_db[table_name].constraints[col]
                        del db_manager.active_db[table_name].constraints_ptr[key]
        if column_pointer:                
            if len(db_manager.active_db[table_name].private_constraints[column_pointer])>1:
                db_manager.active_db[table_name].private_constraints[column_pointer].remove(self.const_name)
            else:
                del db_manager.active_db[table_name].private_constraints[column_pointer]
            print(f'column <{column_pointer}> has been affected')        
            
        if column_pointer is None:
            raise ValueError(f"There is no constraint with this name <{self.const_name}>")
        
            

                
            
            
        
        