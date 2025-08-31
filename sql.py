from adminDatabase import database as DB
class Lexer:
    keywords = ("SELECT", "FROM", "WHERE")
    Comparison_Operators = ("=", "!", "<", ">")
    MainOperators = ("AND", "OR")
    
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
            raise SyntaxError(f"Unexpected character '{char}' at position {self.pos}")
        return self.tokens
            
    def get_operator(self):
        OPT = ""
        while self.pos < len(self.query) and self.query[self.pos] in Lexer.Comparison_Operators:
            OPT += self.query[self.pos]
            self.pos += 1
        return OPT
    def getFullInput(self):
        key = ""
        while self.pos < len(self.query) and (self.query[self.pos].isalnum() or self.query[self.pos] == '_'):
            key += self.query[self.pos]
            self.pos += 1
        return key
    
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
        
class Parser:
    database = DB
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
        left = None
        right = None
        if self.current_token()[0] == "WHERE":
            self.eat("WHERE")
            col = self.eat("IDENTIFIER")[1]
            opt = self.eat("OPT")[1]
            val = self.eat("IDENTIFIER")[1]
            left = Condition(col, opt, val)
            if self.current_token()[0] == "MAINOPT":
                MainOperation = self.eat("MAINOPT")[1].upper()
                col = self.eat("IDENTIFIER")[1]
                opt = self.eat("OPT")[1]
                val = self.eat("IDENTIFIER")[1]
                right = Condition(col, opt, val)
                self.eat("SEMICOLON")
                Logical_condition  = LogicalCondition(left, MainOperation, right)
                ast = SelectStatement(columns, table, Logical_condition)
                return self.execute(ast)
        self.eat("SEMICOLON")
        ast = SelectStatement(columns, table, left)
        return self.execute(ast)  # return result



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
    
    
    def execute(self, ast):
        table_name = ast.table
        if table_name not in Parser.database:
            raise ValueError(f"Table '{table_name}' does not exist")
        table = Parser.database[table_name]
        requested_columns = ast.columns
        if requested_columns == ['*']:
            columns_to_return = table[0].keys() if table else []
        else:
            for col in requested_columns:
                if table and col not in table[0]:
                    raise ValueError(f"Column '{col}' does not exist in table '{table_name}'")
            columns_to_return = requested_columns
        result = []
        for row in table:
            if ast.where is None or self.where_eval(ast.where, row):
                selected_row = {col: row[col] for col in columns_to_return}
                result.append(selected_row)
        return result

    def where_eval(self, where, row):
        if isinstance(where, Condition):
            if type(row[where.column]) == str:
                left = row[where.column].lower()
            else:
                left = row[where.column]
            right = where.value
            op  = where.operator
            if op == "=": return left == right
            if op == "!=": return left != right
            if op == "<": return left < right
            if op == "<=": return left <= right
            if op == ">": return left > right
            if op == ">=": return left >= right
            raise ValueError(f"Unknown operator {op}")
        elif isinstance(where, LogicalCondition):
            MainOperator = where.MainOperator
            left = self.where_eval(where.left, row)
            right = self.where_eval(where.right, row)
            return (left and right if MainOperator.upper() == "AND" else (left or right))

            

        