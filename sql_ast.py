class SelectStatement:
    def __init__(self, columns = None, func = None, table = None, where = None):
        self.columns = columns
        self.func = func
        self.table = table
        self.where = where        
        

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
        
class Functions:
    def __init__(self, arg, alias = "?column?"):
        self.alias = alias
        self.arg = arg
        
class COUNT(Functions):
    def __init__(self, arg, alias="?column?"):
        super().__init__(arg, alias)
    
        
        
c = COUNT("*", "allwors")
print(c.arg)
        
        