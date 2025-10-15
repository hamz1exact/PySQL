import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from utilities import db_manager
from sql_types.sql_types import *
from src.constants import *
from errors import *

db_manager = db_manager

class Lexer:
    keywords = SQL_KEYWORDS
    constraints = CONSTRAINT_TYPES
    nullchecks = NULLCHECKS
    membership = MEMBERSHIP
    absence_of_value = ABSENCE_OF_VALUE
    concat = CONCAT
    between = BETWEEN
    exists = EXISTS
    cast = CAST
    case_when = CASE_WHEN
    date_and_time = DATE_AND_TIME
    extract = EXTRACT
    nullif = NULLIF
    replace = REPLACE
    like = LIKE
    order_by_keys = ORDER_BY_KEYS
    order_by_drc = ORDER_BY_DRC
    group_by_keys = GROUP_BY_KEYS
    having_keys = HAVING_KEYS
    offset_keys = OFFSET_KEYS
    limit_keys = LIMIT_KEYS
    coalesce = COALESCE
    math_functions = MATH_FUNCTIONS
    string_functions = STRING_FUNCTIONS
    math_operations = MATH_OPERATIONS
    restrictions = RESTRICTIONS
    conflict_keywords = CONFLICT_KEYWORDS
    datatypes = DATATYPE_MAPPING
    comparison_operators = COMPARISON_OPERATORS
    logical_operators  = LOGICAL_OPERATORS
    special_characters = SPECIAL_CHARACTERS
    aggregation_functions = AGGREGATION_FUNCTIONS
    set_operators = SET_OPERATORS
    
    

    def __init__(self, query):
        
        self.query = query
        self.pos = 0
        self.tokens = []
        self.Tokenize()

    def Tokenize(self):
        while self.pos < len(self.query):
            char = self.query[self.pos]

            # --- Numbers ---
            if char.isdigit():
                number = self.get_number()
                if self.tokens[-1][0] == TokenTypes.LIMIT:
                    self.tokens.pop()
                    self.tokens.append((TokenTypes.LIMIT, number))
                elif self.tokens[-1][0] == TokenTypes.OFFSET:
                    self.tokens.pop()
                    self.tokens.append((TokenTypes.OFFSET, number))
                else:
                    if self.tokens[-1][1] == "-" and self.tokens[len(self.tokens)-2][0] != TokenTypes.IDENTIFIER:
                        
                        self.tokens.pop()
                        self.tokens.append(("NUMBER", -number))
                    else:
                        self.tokens.append(("NUMBER", number))
                continue
            # --- Identifiers, keywords, booleans, datatypes ---
            if char.isalpha():
                word = self.getFullInput()
                upper_word = word.upper()
                if upper_word in Lexer.keywords:
                    if len(self.tokens)>1 and self.tokens[-1][1] == "DO":
                        self.tokens.append(("ACTION", upper_word))
                    else:
                        self.tokens.append((upper_word, upper_word))
                    
                elif upper_word in Lexer.date_and_time:
                    self.tokens.append((TokenTypes.DATE_AND_TIME, upper_word)) 
                elif upper_word in Lexer.limit_keys:
                    self.tokens.append((TokenTypes.LIMIT, TokenTypes.LIMIT))
                    
                elif upper_word in Lexer.constraints:
                    if upper_word == TokenTypes.NULL:
                        if self.tokens[-1][0] == TokenTypes.NOT and self.tokens[len(self.tokens)-2][1] != TokenTypes.IS:
                            self.tokens.pop()
                            self.tokens.append((TokenTypes.CONSTRAINT, TokenTypes.NOT_NULL))
                        else:
                            self.tokens.append((TokenTypes.NULL, None))
                    elif upper_word == "KEY":
                        if self.tokens[-1][1] == "PRIMARY":
                            self.tokens.pop()
                            self.tokens.append((TokenTypes.CONSTRAINT, TokenTypes.PRIMARY_KEY))
                        else:
                            raise ValueError("Perhaps You miss PRIMARY key word")
                    else:
                        self.tokens.append((TokenTypes.CONSTRAINT, upper_word))
                            
                    
                elif upper_word in Lexer.case_when:
                    self.tokens.append((TokenTypes.CASE_WHEN, upper_word))
                 
                elif upper_word in Lexer.coalesce:
                    self.tokens.append((TokenTypes.COALESCE, TokenTypes.COALESCE))
                
                elif upper_word in Lexer.conflict_keywords:
                    self.tokens.append((TokenTypes.CONFLICT, upper_word))

                elif upper_word in Lexer.restrictions:
                    self.tokens.append((TokenTypes.RESTRICTION, upper_word))
                
                elif upper_word in Lexer.concat:
                    self.tokens.append((TokenTypes.CONCAT, TokenTypes.CONCAT))
                    
                elif upper_word in Lexer.replace:
                    self.tokens.append((TokenTypes.REPLACE, TokenTypes.REPLACE))

                elif upper_word in Lexer.string_functions:
                    self.tokens.append((TokenTypes.STRING_FUNC, upper_word))
                    
                
                elif upper_word in Lexer.offset_keys:
                    self.tokens.append((TokenTypes.OFFSET, TokenTypes.OFFSET))
                    
                elif upper_word in Lexer.math_functions:
                    self.tokens.append((TokenTypes.MATH_FUNC, upper_word))
                    
                elif upper_word in Lexer.nullif:
                    self.tokens.append((TokenTypes.NULLIF, TokenTypes.NULLIF))
                    
                elif upper_word in Lexer.exists:
                    
                    self.tokens.append((TokenTypes.EXISTS,TokenTypes.EXISTS))
                    
                elif upper_word in Lexer.extract:
                    self.tokens.append((TokenTypes.EXTRACT, TokenTypes.EXTRACT)) 
                    
                elif upper_word in Lexer.like:
                    if self.tokens and self.tokens[-1][1] == TokenTypes.NOT:
                        self.tokens.pop()
                        self.tokens.append((TokenTypes.LIKE, TokenTypes.NOT))
                    self.tokens.append((TokenTypes.LIKE, upper_word))
                    
                elif upper_word in Lexer.group_by_keys:
                    self.tokens.append((TokenTypes.GROUP_BY, upper_word))
                    
                elif upper_word in Lexer.between:
                    if self.tokens and self.tokens[-1][1] == TokenTypes.NOT:
                        self.tokens.pop()
                        self.tokens.append((TokenTypes.BETWEEN, TokenTypes.NOT))
                    self.tokens.append((TokenTypes.BETWEEN, upper_word))
                    
                elif upper_word in Lexer.having_keys:
                    self.tokens.append((TokenTypes.HAVING, upper_word))
                    
                elif upper_word in Lexer.cast:
                    self.tokens.append((TokenTypes.CAST, TokenTypes.CAST))
                    
                elif upper_word == TokenTypes.BY:
                    if self.tokens[-1][0] == TokenTypes.ORDER_BY:
                        self.tokens.append((TokenTypes.ORDER_BY, upper_word))
                    elif self.tokens[-1][0] == TokenTypes.GROUP_BY:
                        self.tokens.append((TokenTypes.GROUP_BY, upper_word))
                    else:
                        raise ValueError ("'BY' keyword Should be Followed by either GROUP or ORDER")
                    
                elif upper_word in Lexer.order_by_keys:
                    self.tokens.append((TokenTypes.ORDER_BY, upper_word))
                    
                elif upper_word in Lexer.order_by_drc:
                    self.tokens.append(("ORDER_BY_DRC", upper_word))
                    
                elif upper_word == TokenTypes.NOT:
                    if self.tokens and self.tokens[-1][1] == TokenTypes.IS:
                        self.tokens.append((TokenTypes.NULLCHECK, TokenTypes.NOT))
                    else:
                        self.tokens.append((TokenTypes.NOT, TokenTypes.NOT))
                        
                elif upper_word in Lexer.absence_of_value:
                    self.tokens.append((TokenTypes.NULL, None))
                    
                elif upper_word in Lexer.membership:
                    if self.tokens and self.tokens[-1][1] == TokenTypes.NOT:
                        self.tokens.pop()
                        self.tokens.append((TokenTypes.MEMBERSHIP, TokenTypes.NOT))
                    self.tokens.append((TokenTypes.MEMBERSHIP, upper_word))

                elif upper_word in Lexer.nullchecks:
                    
                    self.tokens.append((TokenTypes.NULLCHECK, upper_word))
                    
                elif upper_word in Lexer.logical_operators:
                    self.tokens.append((TokenTypes.HIGH_PRIORITY_OPERATOR, upper_word))
                    
                elif upper_word in Lexer.datatypes:
                    self.tokens.append(("DATATYPE", upper_word))
                    
                elif word.lower() == "true":
                    self.tokens.append(("BOOLEAN", True))
                    
                elif word.lower() == "false":
                    self.tokens.append(("BOOLEAN", False))
                    
                elif upper_word in Lexer.aggregation_functions:
                    self.tokens.append((TokenTypes.FUNC, upper_word))
                    
                else:
                    if self.tokens[-1][1] == "DO":
                        
                        self.tokens.append(("ACTION", word.upper()))
                    else:
                        self.tokens.append((TokenTypes.IDENTIFIER, word))
                continue

            # --- Strings ---
            if char in ('"', "'"):
                string_value = self.getFullStr()
                self.tokens.append(("STRING", string_value))
                continue

            # --- Parentheses ---
            if char == '(':
                self.tokens.append((TokenTypes.OPEN_PAREN, char))
                self.pos += 1
                continue
            if char == ')':
                self.tokens.append((TokenTypes.CLOSE_PAREN, char))
                self.pos += 1
                continue

            # --- Whitespace ---
            if char in ' \t\n':
                self.pos += 1
                continue
            if char == '.':
                if self.tokens[-1][0] == TokenTypes.IDENTIFIER:
                    ref = self.tokens.pop()[1]
                    self.tokens.append((TokenTypes.REFERENCE, ref))
                self.tokens.append((TokenTypes.DOT, TokenTypes.DOT))
                self.pos += 1
                continue

            # --- Comma ---
            if char == ',':
                self.tokens.append((TokenTypes.COMMA, char))
                self.pos += 1
                continue

            # --- Semicolon ---
            if char == ";":
                self.tokens.append((TokenTypes.SEMICOLON, char))
                self.pos += 1
                continue

            # --- Operators ---
            if char in Lexer.comparison_operators:
                LOW_PRIORITY_OPERATOR = self.get_operator()
                self.tokens.append((TokenTypes.LOW_PRIORITY_OPERATOR, LOW_PRIORITY_OPERATOR))
                continue

            # --- Asterisk (SELECT *) ---
            if char == "*":
                self.tokens.append((TokenTypes.STAR, char))
                self.pos += 1
                continue
            if char in Lexer.math_operations:
                self.tokens.append((TokenTypes.MATH_OPERATOR, char))
                self.pos += 1
                continue

            # --- Unknown character ---
            raise SyntaxError(f"Unexpected character '{char}' at position {self.pos}")
        return self.tokens

    # ----------------- Helper Methods -----------------
    def get_number(self):
        num = ""
        while self.pos < len(self.query) and (self.query[self.pos].isdigit() or self.query[self.pos] == "."):
            num += self.query[self.pos]
            self.pos += 1
        if "." in num:
            return float(num)
        return int(num)

    def getFullInput(self):
        key = ""
        while self.pos < len(self.query) and (self.query[self.pos].isalnum() or self.query[self.pos] in Lexer.special_characters) :
            if self.query[self.pos] == '.':
                break
            key += self.query[self.pos]
            self.pos += 1
        return key

    def getFullStr(self):
        quote_char = self.query[self.pos]
        self.pos += 1  # skip opening quote
        string_val = ""
        while self.pos < len(self.query) and self.query[self.pos] != quote_char:
            string_val += self.query[self.pos]
            self.pos += 1
        self.pos += 1  # skip closing quote
        return string_val

    def get_operator(self):
        LOW_PRIORITY_OPERATOR = ""
        while self.pos < len(self.query) and self.query[self.pos] in Lexer.comparison_operators:
            LOW_PRIORITY_OPERATOR += self.query[self.pos]
            self.pos += 1
        return LOW_PRIORITY_OPERATOR