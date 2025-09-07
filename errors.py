# errors.py
from datatypes import *


class SQLError(Exception):
    """Base class for all SQL-related errors"""
    def __init__(self, message, error_code=None, line=None, column=None):
        self.message = message
        self.error_code = error_code
        self.line = line
        self.column = column
        super().__init__(self.formatted_message())
    
    def formatted_message(self):
        if self.line and self.column:
            return f"{self.message} at line {self.line}, column {self.column}"
        return self.message

# Lexer/Parser errors
class SyntaxError(SQLError):
    def __init__(self, message, line=None, column=None):
        super().__init__(message, "SYNTAX_ERROR", line, column)

class UnexpectedTokenError(SyntaxError):
    def __init__(self, expected, got, line=None, column=None):
        message = f"Expected '{expected}' but got '{got}'"
        super().__init__(message, line, column)

class UnexpectedBehaviors(SQLError):
    def __init__(self, message, column=None):
        super().__init__(message, column)
        

class UnexpectedDataType(UnexpectedBehaviors):
    def __init__(self, column, expected, given):
        message = f"Unexpected Data Type of columns '{column}', Expected {expected} But got {given}"
        super().__init__(message)
        
    

# Database/Table errors
class DatabaseError(SQLError):
    pass

class DatabaseNotFoundError(DatabaseError):
    def __init__(self, db_name):
        super().__init__(f"Database '{db_name}' does not exist", "DB_NOT_FOUND")

class TableNotFoundError(DatabaseError):
    def __init__(self, table_name):
        super().__init__(f"Table '{table_name}' does not exist", "TABLE_NOT_FOUND")

class TableAlreadyExistsError(DatabaseError):
    def __init__(self, table_name):
        super().__init__(f"Table '{table_name}' already exists", "TABLE_EXISTS")

# Column/Data errors
class ColumnError(SQLError):
    pass

class ColumnNotFoundError(ColumnError):
    def __init__(self, column_name, table_name):
        super().__init__(f"Column '{column_name}' does not exist in table '{table_name}'", "COLUMN_NOT_FOUND")

class DataTypeError(ColumnError):
    def __init__(self, column_name, expected_type, got_type, value=None):
        if value:
            message = f"Column '{column_name}' expects {expected_type}, got {got_type}: {value}"
        else:
            message = f"Column '{column_name}' expects {expected_type}, got {got_type}"
        super().__init__(message, "DATA_TYPE_ERROR")

# Constraint errors
class ConstraintError(SQLError):
    pass

class NotNullError(ConstraintError):
    def __init__(self, column_name):
        super().__init__(f"Column '{column_name}' cannot be NULL", "NOT_NULL_VIOLATION")

class UniqueConstraintError(ConstraintError):
    def __init__(self, column_name, value):
        super().__init__(f"Duplicate value '{value}' for UNIQUE column '{column_name}'", "UNIQUE_VIOLATION")
        
x = INT(1)
b = BOOLEAN(True)

raise UnexpectedDataType('age', x.sqltype, b.sqltype)