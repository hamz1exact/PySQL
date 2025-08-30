class Lexer:
    keywords = ("SELECT", "FROM")
    def __init__(self, query):
        self.query = query
        self.pos = 0
        self.tokens = []
        self.Tokenize()
    def Tokenize(self):
        while self.pos < len(self.query):
            char = self.query[self.pos]
            if char.isalpha():
                word = self.getFullInput()
                if word.upper() in Lexer.keywords:
                    self.tokens.append((word.upper(), word.upper()))
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
            if char == "*":
                self.pos += 1
                self.tokens.append("STAR", char)
                continue
            raise SyntaxError(f"Unexpected character '{char}' at position {self.pos}")
        print(self.tokens)
            
            
    def getFullInput(self):
        key = ""
        while self.pos < len(self.query) and (self.query[self.pos].isalnum() or self.query[self.pos] == '_'):
            key += self.query[self.pos]
            self.pos += 1
        return key
    
class SelectStatement:
    def __init__(self, columns, table):
        self.columns = columns
        self.table = table
    
class Parser:
    database = {
        "users": [
            {"id": 1, "name": "Hamza Deraoui", "age": 19, "isStudent": True},
            {"id": 2, "name": "Ali", "age": 21, "isStudent": False},
        ]
    }

    def __init__(self, tokens):
        self.tokens = tokens
        self.pos = 0

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
        self.eat("SEMICOLON")
        ast = SelectStatement(columns, table[1])
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
        return self.eat("IDENTIFIER")

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
            selected_row = {col: row[col] for col in columns_to_return}
            result.append(selected_row)
        return result
                
            