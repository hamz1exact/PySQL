"""
Constants for SQL Engine - keeps all string literals in one place
"""

from datatypes import *

# SQL Keywords - moved from engine.py
SQL_KEYWORDS = (
    "SELECT", "FROM", "WHERE", "INSERT", "INTO", "VALUES",
    "UPDATE", "SET", "DELETE", "CREATE", "DATABASE", "TABLE",
    "USE", "DEFAULT", "ALIAS", "AS", "DISTINCT", "SHOW", "UNION",
    "ALL", "INTERSECT", "EXCEPT", "RETURNING", "VIEW", "AS", "CALL",
    "DATA", "WITH", "NO", "VIEWS", "MATERIALIZED", "REFRESH"
)

# Data type mapping - moved from engine.py  
DATATYPE_MAPPING = {
    "INT": INT,
    "INTEGER": INT,
    "STRING": VARCHAR,
    "FLOAT": FLOAT,
    "BOOLEAN": BOOLEAN,
    "CHAR": CHAR,
    "VARCHAR": VARCHAR,
    "TEXT": TEXT,
    "SERIAL": SERIAL,
    "DATE": DATE,
    "TIME": TIME,
    "TIMESTAMP": TIMESTAMP,
}

# Token type strings - no need to change these, just centralize them
class TokenTypes:
    SELECT = "SELECT"
    INSERT = "INSERT"
    UPDATE = "UPDATE"
    DELETE = "DELETE"
    CREATE = "CREATE"
    USE = "USE"
    
    IDENTIFIER = "IDENTIFIER"
    NUMBER = "NUMBER"
    STRING = "STRING"
    BOOLEAN = "BOOLEAN"
    
#   token type names
    DATABASE = "DATABASE"
    WHERE = "WHERE"
    UNION = "UNION"
    INTO = "INTO"
    UNION_ALL = "UNION ALL"
    ALL = "ALL"
    FROM = "FROM"
    DISTINCT = "DISTINCT"
    TABLE = "TABLE"
    GROUP_BY = "GROUP_BY_KEY"
    OFFSET = "OFFSET"
    ORDER_BY  = "ORDER_BY_KEY"
    AS = "AS"
    CONFLICT = "CONF"
    HAVING = "HAVING"
    FROM = "FROM"
    LIMIT = "LIMIT"
    BY = "BY"
    MATH_FUNC = "MATH_FUNC"
    VALUES = "VALUES"
    FUNC = "FUNC"
    EXCEPT = "EXCEPT"
    INTERSECT = "INTERSECT"
    SET = "SET"
    DATEDIFF = "DATEDIFF"
    RETURNING = "RETURNING"
    CASE_WHEN = "CASE_WHEN"
    CAST = "CAST"
    DEFAULT = "DEFAULT"
    ACTION = "ACTION"
    STRING_FUNC = "STRING_FUNC"
    DOT = "DOT"
    DISTINCT = "DISTINCT"
    FUNC = "FUNC"
    EXTRACT = "EXTRACT"
    CONSTRAINT = "CONST"
    STAR = "STAR"
    EXISTS = "EXISTS"
    NULLIF = "NULLIF"
    REFERENCE = "REFERENCE"
    SHOW = "SHOW"
    RESTRICTION = "RESTS"
    CONSTRAINTS = "CONSTRAINTS"
    DATE_AND_TIME = "DATE_AND_TIME"
    CURRENT_DATE = "CURRENT_DATE"
    CURRENT_TIME = "CURRENT_TIME"
    CONCAT = "CONCAT"
    CHECK = "CHECK"
    COALESCE = "COALESCE"
    REFRESH = "REFRESH"
    PRIMARY_KEY = "PRIMARY KEY"
    NOW = "NOW"
    UNIQUE = "UNIQUE"
    NOT_NULL = "NOT NULL"
    ASCENDING_ORDER = "ASC"
    VIEW = "VIEW"
    REPLACE = "REPLACE"
    DESCENDING_ORDER = "DESC"
    VIEWS = "VIEW"
    SERIAL = "SERIAL"
    LIKE = "LIKE"
    NOT = "NOT"
    NULLCHECK = "NULLCHECK"
    CALL = "CALL"
    MATERIALIZED = "MATERIALIZED"
    STRING_FUNC = "STRING_FUNC"
    LOW_PRIORITY_OPERATOR = "LOW_PRIORITY_OPERATOR"
    HIGH_PRIORITY_OPERATOR = "HIGH_PRIORITY_OPERATOR"
    SEMICOLON = "SEMICOLON"
    NO = "NO"
    BETWEEN = "BETWEEN"
    DATA = "DATA"
    MEMBERSHIP = "MEMBERSHIP"
    COMMA = "COMMA"
    OPEN_PAREN = "OPEN_PAREN"
    WITH = "WITH"
    
    NULL = "NULL"
    MATH_OPERATOR = "MATH_OPERATOR"
    CLOSE_PAREN = "CLOSE_PAREN"

# Constraint types
CONSTRAINT_TYPES = {"NULL", "PRIMARY", "UNIQUE", "KEY"}

# Set operatos

SET_OPERATORS = ("UNION", "UNION ALL", "INTERSECT", "EXCEPT")

# Math operations
MATH_OPERATIONS = {"*", "/", "+", "-"}

# Comparison operators  
COMPARISON_OPERATORS = ("=", "!", "<", ">")

# Logical operators
LOGICAL_OPERATORS = ("AND", "OR")

ABSENCE_OF_VALUE = ("NONE", "NULL","EMPYY")

SPECIAL_CHARACTERS = ("@", "_", "+", "-", ".")

NULLCHECKS = ("IS")

MEMBERSHIP = ("concat")

EXISTS = ("EXISTS")

BETWEEN = ("BETWEEN")

ON_INSERT_UPDATE_ACCEPTED_DATA_TYPES = ("NUMBER", "STRING", "BOOLEAN")

CAST = ("CAST")

CONCAT = ("CONCAT")

CASE_WHEN = {
    
    "CASE",
    "WHEN",
    "THEN",
    "ELSE",
    "END"
}

DATE_AND_TIME = {
    "YEAR",
    "MONTH",
    "DAY",
    "HOUR",
    "MINUTE",
    "SECOND",
    "CURRENT_DATE",
    "NOW",
    "DATEDIFF",
    "CURRENT_TIME"
}

AGGREGATION_FUNCTIONS = { 
        "COUNT",
        "SUM",
        "MAX",
        "MIN",
        "AVG"
        }

EXTRACT = {"EXTRACT"}

NULLIF = {"NULLIF"}

REPLACE = {"REPLACE"}

LIKE = ("LIKE")

ORDER_BY_KEYS ={"ORDER"}

ORDER_BY_DRC = {"ASC", "DESC"}

GROUP_BY_KEYS = {"GROUP"}

HAVING_KEYS = {"HAVING"}

OFFSET_KEYS = {"OFFSET"}

LIMIT_KEYS = {"LIMIT"}

COALESCE = ("COALESCE")

MATH_FUNCTIONS = {
    
    "ABS",
    "ROUND",
    "CEIL",
    "FLOOR"
}

STRING_FUNCTIONS = {
    
    "UPPER",
    "LOWER",
    "LENGTH",
    "SUBSTRING",
    "REVERSE"
}

RESTRICTIONS = ("CHECK")

CONFLICT_KEYWORDS = ("ON", "CONFLICT", "DO")