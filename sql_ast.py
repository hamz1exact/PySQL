class SelectStatement:
    def __init__(self, columns = None, special_columns = None, table = None, where = None, distinct = False, order_by = None, group_by = None, having = None, offset = None, limit = None):
        
        self.columns = columns
        self.special_columns = special_columns
        self.table = table
        self.where = where        
        self.distinct = distinct
        self.order_by = order_by
        self.group_by = group_by
        self.having = having
        self.offset = offset
        self.limit = limit
        


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
        
        