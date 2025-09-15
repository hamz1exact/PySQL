import errors

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
        if self.column_name == "*":
            return row

        return row[self.column_name].value

    def get_referenced_columns(self):
        return {self.column_name}

class LiteralExpression(Expression):
    def __init__(self, value):
        self.value = value
    
    def evaluate(self, row, schema, expected_type = None):
        if expected_type:
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
    def __init__(self, name, expression, alias = None, distinct=False):
        self.name = name        # "SUM", "COUNT", etc.
        self.expression = expression  # ColumnExpression, BinaryOperator, Literal
        self.alias = alias
        self.distinct = distinct

    def evaluate(self, rows, table_schema):
        # 1. Evaluate the expression for each row

        values = [self.expression.evaluate(row, table_schema) for row in rows]
        
        # 2. Handle DISTINCT
        
        if self.distinct and type(values[0]) == dict:
            raise ValueError("'*' Not Supported In This Expression")
        if self.distinct:
            values = list(set(values))

        # 3. Execute function
        if self.name == "COUNT":
            return len(values)
        elif self.name == "SUM":
            if not all(isinstance(v, (int, float)) for v in values):
                raise ValueError("SUM works only with numeric values")
            return sum(values)
        elif self.name == "AVG":
            return sum(values) / len(values)
        elif self.name == "MAX":
            return max(values)
        elif self.name == "MIN":
            return min(values)
        # You can extend: AVG, MIN, MAX, etc.

class GroupBy(Expression):
    def __init__(self, expressions):
        self.expressions = expressions


class WhereClause(Expression):
    def __init__(self, left, operator, right):
        self.left = left
        self.operator = operator
        self.right = right

    def evaluate(self, row, schema):
        # If left is a column and right is literal → coerce literal
        if isinstance(self.left, ColumnExpression) and isinstance(self.right, LiteralExpression):
            col_type = schema[self.left.column_name]  # e.g. AGE, DATE
            left = self.left.evaluate(row, schema)
            right = self.right.evaluate(row, schema, expected_type=col_type)

        # If right is column and left is literal → coerce literal
        elif isinstance(self.right, ColumnExpression) and isinstance(self.left, LiteralExpression):
            col_type = schema[self.right.column_name]
            left = self.left.evaluate(row, schema, expected_type=col_type)
            right = self.right.evaluate(row, schema)

        else:
            left = self.left.evaluate(row, schema)
            right = self.right.evaluate(row, schema)
        if type(right)  == str and type(left) == str:
            left = left.lower()
            right = right.lower()
        if self.operator == '=': return left == right
        elif self.operator == ">": return left  > right
        elif self.operator == "<": return left < right
        elif self.operator == "<=": return left <= right
        elif self.operator == ">=": return right >= right
        elif self.operator == "!=": return left != right
        elif self.operator == "AND": return left and right
        elif self.operator == "OR": return left or  right
        

class Between(Expression):
    def __init__(self, expression, lower, upper, is_not = False):
        self.expression = expression
        self.lower = lower
        self.upper = upper
        self.is_not = is_not

    def evaluate(self, row, schema):
        
        expected_type = schema[self.expression.column_name]
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
        
        
        
        
    