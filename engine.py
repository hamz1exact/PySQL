from sql_ast import *
from executor import execute 
from datetime import datetime
from database_manager import DatabaseManager, Table
import re
from checker import *
from datatypes import *


db_manager = DatabaseManager()

class Lexer:
    # Keywords and data types
    keywords = (
        "SELECT", "FROM", "WHERE", "INSERT", "INTO", "VALUES",
        "UPDATE", "SET", "DELETE", "CREATE", "DATABASE", "TABLE",
        "USE", "DEFAULT", "ALIAS", "AS")
    AbsenceOfValue = {
        "NONE", "NULL", "EMPTY"
    }
    nullchecks = {
        "IS", "NOT"
    }
    functions = {"COUNT", "SUM", "MAX", "MIN"}
    datatypes = {
        "INT": INT,
        "INTEGER": INT,
        "STRING":VARCHAR,
        "FLOAT": FLOAT,
        "BOOLEAN": BOOLEAN,
        "CHAR": CHAR,
        "VARCHAR": VARCHAR,
        "TEXT": TEXT,
        "SERIAL": SERIAL,
        "DATE": DATE,
        "TIME": TIME,
    }
    Comparison_Operators = ("=", "!", "<", ">")
    MainOperators = ("AND", "OR")
    SpecialCharacters = ("@", "_", "+", "-", ".")

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
                self.tokens.append(("NUMBER", number))
                continue

            # --- Identifiers, keywords, booleans, datatypes ---
            if char.isalpha():
                word = self.getFullInput()
                upper_word = word.upper()
                if upper_word in Lexer.keywords:
                    self.tokens.append((upper_word, upper_word))
                elif upper_word in Lexer.AbsenceOfValue:
                    self.tokens.append(("NONE", upper_word))
                elif upper_word in Lexer.nullchecks:
                    if upper_word == "IS":
                        self.tokens.append(("OPT", "="))
                    elif upper_word == "NOT":
                        if self.tokens and  self.tokens[-1] == ("OPT", "="):
                            self.tokens.pop()
                            self.tokens.append(("OPT", "!="))
                        else:
                            self.tokens.append(("OPT", "!="))
                elif upper_word in Lexer.MainOperators:
                    self.tokens.append(("MAINOPT", upper_word))
                elif upper_word in Lexer.datatypes:
                    self.tokens.append(("DATATYPE", upper_word))
                elif word.lower() == "true":
                    self.tokens.append(("BOOLEAN", True))
                elif word.lower() == "false":
                    self.tokens.append(("BOOLEAN", False))
                elif upper_word in Lexer.functions:
                    self.tokens.append(("FUNC", upper_word))
                else:
                    self.tokens.append(("IDENTIFIER", word))
                continue

            # --- Strings ---
            if char in ('"', "'"):
                string_value = self.getFullStr()
                self.tokens.append(("STRING", string_value))
                continue

            # --- Parentheses ---
            if char == '(':
                self.tokens.append(("OPDBK", char))
                self.pos += 1
                continue
            if char == ')':
                self.tokens.append(("CLDBK", char))
                self.pos += 1
                continue

            # --- Whitespace ---
            if char in ' \t\n':
                self.pos += 1
                continue

            # --- Comma ---
            if char == ',':
                self.tokens.append(("COMMA", char))
                self.pos += 1
                continue

            # --- Semicolon ---
            if char == ";":
                self.tokens.append(("SEMICOLON", char))
                self.pos += 1
                continue

            # --- Operators ---
            if char in Lexer.Comparison_Operators:
                opt = self.get_operator()
                self.tokens.append(("OPT", opt))
                continue

            # --- Asterisk (SELECT *) ---
            if char == "*":
                self.tokens.append(("STAR", char))
                self.pos += 1
                continue

            # --- Unknown character ---
            raise SyntaxError(f"Unexpected character '{char}' at position {self.pos}")
        print(self.tokens)
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
        while self.pos < len(self.query) and (self.query[self.pos].isalnum() or self.query[self.pos] in Lexer.SpecialCharacters):
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
        opt = ""
        while self.pos < len(self.query) and self.query[self.pos] in Lexer.Comparison_Operators:
            opt += self.query[self.pos]
            self.pos += 1
        return opt
    
 
class Parser:
    def __init__(self, tokens):
        self.tokens = tokens
        self.pos = 0 
        self.uses_wildcard = None

    def current_token(self):
        if self.pos < len(self.tokens):
            return self.tokens[self.pos]
        return None

    def eat(self, token_type):
        token = self.current_token()
        if token is None:
            raise SyntaxError(f"Unexpected end of input, expected {token_type}")
        actual_type, actual_value = token
        if actual_type == token_type:
            self.pos += 1
            return token
        else:
            raise SyntaxError(f"Expected token type '{token_type}', got '{actual_type}' at position {self.pos}")

    def parse_select_statement(self):
        self.eat("SELECT")
        columns = self.parse_columns()
        self.eat("FROM")
        table = self.parse_table()
        where = None
        token = self.current_token()
        if token and token[0] == "WHERE":
            self.eat("WHERE")
            where = self.parse_condition_tree()
        self.eat("SEMICOLON")

        return SelectStatement(columns, table, where)


              
    def parse_columns(self):
        token = self.current_token()
        if token[0] == "STAR":
            self.eat("STAR")
            return ["*"]
        columns = []
        alias = "?column?"
        opn = False
        if self.current_token() and self.current_token()[0] == "OPDBK":
            self.eat("OPDBK")
            opn = True
        while True:
            if self.current_token() and self.current_token()[0] == "FUNC":
                func_name = self.eat("FUNC")[1]
                self.eat("OPDBK")
                if self.current_token() and self.current_token()[0] == "STAR":
                    arg = self.eat("STAR")[1]
                else:
                    arg = self.eat("IDENTIFIER")[1]
                self.eat("CLDBK")
                if self.current_token() and self.current_token()[0] == "AS":
                    self.eat("AS")
                    if self.current_token()[0] == "STRING": alias = self.eat("STRING")[1]
                    elif self.current_token()[0] == "IDENTIFIER": alias = self.eat("IDENTIFIER")[1]
                    else:
                        raise ValueError(f"Couldn't use {self.current_token()[1]} as an Alias, please use your alias inside -> ''")
                columns.append(FunctionCall(func_name, arg, alias))
                break
                
            elif self.current_token() and self.current_token()[0] == "IDENTIFIER":
                var = self.eat("IDENTIFIER")[1]
                columns.append(var)
            
            else:
                raise SyntaxError(f"Expected column name or function, got {self.current_token()[0]}")
            
            if self.current_token() and self.current_token()[0] == "COMMA":
                self.eat("COMMA")
                continue
            else:
                if opn:
                    self.eat("CLDBK")
                break

        return columns
                



    def parse_table(self):
        return self.eat("IDENTIFIER")[1]
    
    

    def parse_insert_statement(self):
        self.eat("INSERT")
        self.eat("INTO")
        table = self.parse_insert_table()
        columns = self.parse_insert_columns()
        self.eat("VALUES")
        values = self.parse_insert_values()
        self.eat("SEMICOLON")
        return InsertStatement(table, columns, values)
    
    def parse_insert_table(self):
        return self.eat("IDENTIFIER")[1]
    
    

    def parse_insert_columns(self):
        self.eat("OPDBK")
        columns = []
        while self.current_token() and  (self.current_token()[0] == "IDENTIFIER" or self.current_token()[0] == "COMMA"):
            if self.current_token()[0] != 'COMMA':
                columns.append(self.eat("IDENTIFIER")[1])
            else:
                self.eat("COMMA")
        self.eat("CLDBK")
        return columns
    
    
    def parse_insert_values(self):
        self.eat("OPDBK")  # (
        values = []

        while self.current_token() and self.current_token()[0] != "CLDBK":
            # Skip commas
            if self.current_token()[0] == "COMMA":
                self.eat("COMMA")
                continue

            # Accept type-aware values
            token_type, token_value = self.current_token()
            if token_type in ("NUMBER", "STRING", "BOOLEAN"):
                val = self.eat(token_type)[1]
            else:
                raise SyntaxError(
                    f"Unexpected token type '{token_type}' in VALUES clause. Must be NUMBER, STRING, or BOOLEAN."
                )
            values.append(val)

        self.eat("CLDBK")  # )
        return values

    def parse_condition_tree(self):
        # --- Parse the left condition ---
        col = self.eat("IDENTIFIER")[1]      # Column name
        op  = self.eat("OPT")[1]             # Operator (=, >, <, etc.)

        # --- Determine the type of value ---
        token_type, token_value = self.current_token()
        if token_type in ("NUMBER", "STRING", "BOOLEAN"):
            val = self.eat(token_type)[1]
        elif token_type in Lexer.AbsenceOfValue:
            val = self.eat(token_type)[1]
        else:
            raise SyntaxError(f"Unexpected token type '{token_type}' in WHERE clause for value")

        left_node = Condition(col, op, val)

        # --- Check for logical operator (AND/OR) ---
        if self.current_token() and self.current_token()[0] == "MAINOPT":
            operator = self.eat("MAINOPT")[1]
            # Recursively parse the right side of the condition
            right_node = self.parse_condition_tree()
            return LogicalCondition(left_node, operator, right_node)
        else:
            return left_node
        
        
    def parse_update_statement(self):
        self.eat("UPDATE")
        table_name = self.eat("IDENTIFIER")[1]
        self.eat("SET")
        columns = self.parse_update_columns()
        where = None
        curr_token  = self.current_token()
        if curr_token and curr_token[0] == "WHERE":
            self.eat("WHERE")
            where = self.parse_condition_tree()
        self.eat("SEMICOLON")
        return UpdateStatement(table_name, columns, where)

    def parse_update_columns(self):
        columns = {}

        while self.current_token() and self.current_token()[0] != "SEMICOLON" and self.current_token()[0] not in Lexer.keywords:
            # Skip commas
            if self.current_token()[0] == "COMMA":
                self.eat("COMMA")
                continue

            # Column name
            col = self.eat("IDENTIFIER")[1]

            # Assignment operator
            opt = self.eat("OPT")[1]
            if opt not in ("=", "=="):
                raise SyntaxError(f"Invalid assignment operator '{opt}' for column '{col}'. Use '=' or '=='.")

            # Value token (type-aware)
            token_type, token_value = self.current_token()
            if token_type in ("NUMBER", "STRING", "BOOLEAN"):
                val = self.eat(token_type)[1]
            else:
                raise SyntaxError(
                    f"Unexpected token type '{token_type}' as value for column '{col}' in UPDATE statement."
                )

            columns[col] = val

        return columns

    def parse_delete_statement(self):
        self.eat("DELETE")
        self.eat("FROM")
        table = self.eat("IDENTIFIER")[1]
        token = self.current_token()
        where = None
        if token and token[0] == "WHERE":
            self.eat("WHERE")
            where = self.parse_condition_tree()
        self.eat("SEMICOLON")
        return DeleteStatement(table, where)
    
    
    def parse_create_database(self):
        self.eat("CREATE")
        self.eat("DATABASE")
        db_name = self.eat("IDENTIFIER")[1]
        self.eat("SEMICOLON")
        return CreateDatabseStatement(db_name)



    def parse_create_table(self):
        self.eat("CREATE")
        self.eat("TABLE")
        table_name = self.eat("IDENTIFIER")[1]
        table_name = table_name.strip()
        self.eat("OPDBK")  # (
        schema = {}
        auto = {}
        defaults = {} 
        is_serial = False
        while self.current_token() and self.current_token()[0] != "CLDBK":
            # Column name
            col_name = self.eat("IDENTIFIER")[1]
            # Column type
            col_type = self.eat("DATATYPE")[1]
            if col_type in Lexer.datatypes:
                schema[col_name] = Lexer.datatypes[col_type]
            else:
                raise ValueError(f"Unknown Datatype -> {col_type}")
            # Handle SERIAL
            if col_type.upper() == "SERIAL":
                auto[col_name] = Lexer.datatypes["SERIAL"]()
                is_serial = True
            else:
                is_serial = False

            # Handle DEFAULT values
            if self.current_token() and self.current_token()[0] == "DEFAULT":
                if is_serial:
                    raise ValueError(f"Invalid DEFAULT for column '{col_name}', SERIAL columns cannot have explicit default values.")
                
                self.eat("DEFAULT")
                opt = self.eat("OPT")[1]
                if opt not in ("=", "=="):
                    raise ValueError("Syntax error in DEFAULT clause: expected '=' or '==' after DEFAULT keyword")
                token_type, token_value = self.current_token()
                if token_type:
                    default_value = self.eat(token_type)[1]
                    defaults[col_name] = schema[col_name](default_value)
            # Skip comma if present
            if self.current_token() and self.current_token()[0] == "COMMA":
                self.eat("COMMA")
            print(col_name)

        self.eat("CLDBK")  # )
        self.eat("SEMICOLON")
        return CreateTableStatement(table_name, schema, defaults, auto)
        


    def parse_use_statement(self):
        self.eat("USE")
        db_name = self.eat("IDENTIFIER")[1]
        self.eat("SEMICOLON")
        return UseStatement(db_name)

        
        