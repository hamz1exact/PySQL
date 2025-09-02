from ast import LogicalCondition, Condition, SelectStatement, InsertStatement, UpdateStatement
from executor import execute 

class Lexer:
    keywords = ("SELECT", "FROM", "WHERE", "INSERT", "INTO", "VALUES", "UPDATE", "SET")
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
            if char.isalnum():
                word = self.getFullInput()
                if word.upper() in Lexer.keywords:
                    self.tokens.append((word.upper(), word.upper()))
                elif word.upper() in Lexer.MainOperators:
                    self.tokens.append(("MAINOPT", word.upper()))
                else:
                    self.tokens.append(("IDENTIFIER", word))
                continue
            if char == "'" or char == '"':
                word = self.getFullStr()
                self.tokens.append(("IDENTIFIER", word))
                continue
            if char == '(':
                self.tokens.append(("OPDBK", char))
                self.pos += 1
                continue
            if char == ')':
                self.tokens.append(("CLDBK", char))
                self.pos += 1
                continue
            if char in ' \t\n':
                self.pos += 1
                continue
            if char == ',':
                self.tokens.append(("COMMA", char))
                self.pos += 1
                continue
            if char == ";":
                self.tokens.append(("SEMICOLON", char))
                self.pos += 1
                continue
            if self.query[self.pos] in Lexer.Comparison_Operators:
                OPT = self.get_operator()
                self.tokens.append(("OPT", OPT))
                continue
            if char == "*":
                self.pos += 1
                self.tokens.append(("STAR", char))
                continue
            raise SyntaxError(f"Unexpected character '{char}' at position {self.pos}, Try Your Input inside -> "" ")
        return self.tokens

            
    def get_operator(self):
        OPT = ""
        while self.pos < len(self.query) and self.query[self.pos] in Lexer.Comparison_Operators:
            OPT += self.query[self.pos]
            self.pos += 1
        return OPT
    
    def getFullStr(self):
        quote_char = self.query[self.pos]   # either ' or "
        self.pos += 1  # skip opening quote
        key = ""
        while self.pos < len(self.query) and self.query[self.pos] != quote_char:
            key += self.query[self.pos]
            self.pos += 1
        self.pos += 1  # skip closing quote
        return key
    
    def getFullInput(self):
        key = ""
        while self.pos < len(self.query) and (self.query[self.pos].isalnum() or self.query[self.pos] in Lexer.SpecialCharacters):
            key += self.query[self.pos]
            self.pos += 1
        return key
    
 
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
        while True:
            var = self.eat("IDENTIFIER")
            columns.append(var[1])
            token = self.current_token()
            if token is not None and token[0] == "COMMA":
                self.eat("COMMA")
            else:
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
        self.eat("OPDBK")
        values = []
        while self.current_token() and (self.current_token()[0] == "IDENTIFIER" or self.current_token()[0] == "COMMA"):
           if self.current_token()[0] != 'COMMA':
                values.append(self.eat("IDENTIFIER")[1])
           else:
               self.eat("COMMA")
        self.eat("CLDBK")
        return values

    def parse_condition_tree(self):
        col = self.eat("IDENTIFIER")[1]
        op  = self.eat("OPT")[1]
        val = self.eat("IDENTIFIER")[1]
        left_node = Condition(col, op, val)
        if self.current_token() and self.current_token()[0] == "MAINOPT":
            operator = self.eat("MAINOPT")[1]
            right_node = self.parse_condition_tree()
            return LogicalCondition(right_node, operator, left_node)
        else:
            return left_node
    def parse_update_statement(self):
        self.eat("UPDATE")
        table_name = self.eat("IDENTIFIER")[1]
        self.eat("SET")
        columns = self.parse_update_columns()
        where = None
        if self.current_token()[0] == "WHERE":
            self.eat("WHERE")
            where = self.parse_condition_tree()
        self.eat("SEMICOLON")
        return UpdateStatement(table_name, columns, where)

    def parse_update_columns(self):
        columns = {}
        while self.current_token() and self.current_token()[0] != "SEMICOLON" and self.current_token()[0] not in Lexer.keywords:
            if self.current_token()[0] == "COMMA":
                self.eat("COMMA")
            col = self.eat("IDENTIFIER")[1]
            opt = self.eat("OPT")[1]
            val = self.eat("IDENTIFIER")[1]
            if opt not in ("=", "=="):
                raise SyntaxError(f"Invalid assignment operator '{opt}' for column '{col}'. Use '=' or '=='.")
            columns[col] = val
        return columns






