class SelectStatement:
    def __init__(self, columns, table, where = None):
        self.columns = columns
        self.table = table
        self.where = where

class Condition:
    def __init__(self, column, operator, value):
        self.column = column
        self.operator = operator
        if value.isdigit():
            self.value = int(value)
        elif value.lower() == 'true':
            self.value = True
        elif value.lower() == 'false':
            self.value = False
        else:
            self.value = value.lower()
            
            
class LogicalCondition:
    def __init__(self, left, MainOperator, right):
        self.left = left
        self.MainOperator = MainOperator
        self.right = right