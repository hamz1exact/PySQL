from sql_ast import *
from executor import execute 
from datetime import datetime
from database_manager import DatabaseManager, Table
from datatypes import *


db_manager = DatabaseManager()

class Lexer:
    # Keywords and data types
    keywords = (
        "SELECT", "FROM", "WHERE", "INSERT", "INTO", "VALUES",
        "UPDATE", "SET", "DELETE", "CREATE", "DATABASE", "TABLE",
        "USE", "DEFAULT", "ALIAS", "AS", "DISTINCT")
    AbsenceOfValue = {
        "NONE", "NULL", "EMPTY"
    }
    nullchecks = {
        "IS"
    }
    membership = {
        "IN"
    }
    between = {
        "BETWEEN"
    }
    like = {
        "LIKE"
    }
    order_by_keys ={
        "ORDER"
    }
    order_by_drc = {
        "ASC", "DESC"
    }
    group_by_keys = {
            "GROUP"
            }
    having_keys = {
        "HAVING"
    }
    offset_keys = {
        "OFFSET"
    }
    limit_keys = {
        "LIMIT"
    }
    
    math_operations = {
        
        "*",
        "/",
        "+",
        "-"
            }
    
    functions = {
        
        "COUNT",
        "SUM",
        "MAX",
        "MIN",
        "AVG"
        
        }
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
                if self.tokens[-1][0] == "LIMIT":
                    self.tokens.pop()
                    self.tokens.append(("LIMIT", number))
                elif self.tokens[-1][0] == "OFFSET":
                    self.tokens.pop()
                    self.tokens.append(("OFFSET", number))
                else:
                    self.tokens.append(("NUMBER", number))
                continue
            # --- Identifiers, keywords, booleans, datatypes ---
            if char.isalpha():
                word = self.getFullInput()
                upper_word = word.upper()
                if upper_word in Lexer.keywords:
                    self.tokens.append((upper_word, upper_word))
                    
                elif upper_word in Lexer.limit_keys:
                    self.tokens.append(("LIMIT", "LIMIT"))
                
                elif upper_word in Lexer.offset_keys:
                    self.tokens.append(("OFFSET", "OFFSET"))
                
                elif upper_word in Lexer.like:
                    if self.tokens and self.tokens[-1][1] == "NOT":
                        self.tokens.pop()
                        self.tokens.append(("LIKE", "NOT"))
                    self.tokens.append(("LIKE", upper_word))
                    
                elif upper_word in Lexer.group_by_keys:
                    self.tokens.append(("GROUP_BY_KEY", upper_word))
                    
                elif upper_word in Lexer.between:
                    if self.tokens and self.tokens[-1][1] == "NOT":
                        self.tokens.pop()
                        self.tokens.append(("BETWEEN", "NOT"))
                    self.tokens.append(("BETWEEN", upper_word))
                    
                elif upper_word in Lexer.having_keys:
                    self.tokens.append(("HAVING", upper_word))
                    
                elif upper_word == "BY":
                    if self.tokens[-1][0] == "ORDER_BY_KEY":
                        self.tokens.append(("ORDER_BY_KEY", upper_word))
                    elif self.tokens[-1][0] == "GROUP_BY_KEY":
                        self.tokens.append(("GROUP_BY_KEY", upper_word))
                    else:
                        raise ValueError ("'BY' keyword Should be Followed by either GROUP or ORDER")
                    
                elif upper_word in Lexer.order_by_keys:
                    self.tokens.append(("ORDER_BY_KEY", upper_word))
                    
                elif upper_word in Lexer.order_by_drc:
                    self.tokens.append(("ORDER_BY_DRC", upper_word))
                    
                elif upper_word == "NOT":
                    if self.tokens and self.tokens[-1][1] == "IS":
                        self.tokens.append(("NULLCHECK", "NOT"))
                    else:
                        self.tokens.append(("NOT", "NOT"))
                        
                elif upper_word in Lexer.AbsenceOfValue:
                    self.tokens.append(("NULL", None))
                    
                elif upper_word in Lexer.membership:
                    if self.tokens and self.tokens[-1][1] == "NOT":
                        self.tokens.pop()
                        self.tokens.append(("MEMBERSHIP", "NOT"))
                    self.tokens.append(("MEMBERSHIP", upper_word))

                elif upper_word in Lexer.nullchecks:
                    
                    self.tokens.append(("NULLCHECK", upper_word))
                    
                elif upper_word in Lexer.MainOperators:
                    self.tokens.append(("HIGH_PRIORITY_OPERATOR", upper_word))
                    
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
                if self.tokens and self.tokens[-1][1] == "NOT":
                    self.tokens.pop()
                    self.tokens.append(("NegationCondition", "NOT"))
                self.tokens.append(("OPEN_PAREN", char))
                self.pos += 1
                continue
            if char == ')':
                self.tokens.append(("CLOSE_PAREN", char))
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
                LOW_PRIORITY_OPERATOR = self.get_operator()
                self.tokens.append(("LOW_PRIORITY_OPERATOR", LOW_PRIORITY_OPERATOR))
                continue

            # --- Asterisk (SELECT *) ---
            if char == "*":
                self.tokens.append(("STAR", char))
                self.pos += 1
                continue
            if char in Lexer.math_operations:
                self.tokens.append(("MATH_OPERATOR", char))
                self.pos += 1
                continue

            # --- Unknown character ---
            raise SyntaxError(f"Unexpected character '{char}' at position {self.pos}")
        
        # print(self.tokens)
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
        LOW_PRIORITY_OPERATOR = ""
        while self.pos < len(self.query) and self.query[self.pos] in Lexer.Comparison_Operators:
            LOW_PRIORITY_OPERATOR += self.query[self.pos]
            self.pos += 1
        return LOW_PRIORITY_OPERATOR
    
 
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
        where = None
        unique = False
        order_in = []
        group_in = []
        having_in = None
        limit, offset = None, None
        self.eat("SELECT")
        if self.current_token() and self.current_token()[0] == "DISTINCT":
            self.eat("DISTINCT")
            unique = True
        columns, function_columns = self.parse_columns()
        self.eat("FROM")
        table = self.parse_table()
        token = self.current_token()
        if token and token[0] == "WHERE":
            self.eat("WHERE")
            where = self.parse_condition_tree()
        if self.current_token() and self.current_token()[0] == "GROUP_BY_KEY":
            group_in = self.group_by()
            if self.current_token() and self.current_token()[0] == "HAVING":
                self.eat("HAVING")
                having_in = self.parse_having()
        if self.current_token() and self.current_token()[0] == "ORDER_BY_KEY":
            order_in = self.order_by()
        if self.current_token() and self.current_token()[0] == "OFFSET":
            raise ValueError("The OFFSET clause must be used with a LIMIT clause.")
        if self.current_token() and self.current_token()[0] == "LIMIT":
            limit = self.eat("LIMIT")[1]
            if self.current_token() and self.current_token()[0] == "OFFSET":
                offset = self.eat("OFFSET")[1]
        self.eat("SEMICOLON")
        return SelectStatement(columns, function_columns, table, where, distinct = unique, order_by = order_in, group_by = group_in, having = having_in, limit=limit, offset=offset)


              
    def parse_columns(self):
        columns = []
        function_columns = []
        alias = None
        while True:
            if self.current_token()[0] == "STAR":
                self.eat("STAR")
                if self.current_token() and self.current_token()[0] == "AS":
                    raise ValueError ("'*' select all take no alias")
                columns.append(Columns("*", None))
                
            elif self.current_token()[0] == "IDENTIFIER":
                col = self.eat("IDENTIFIER")[1]
                alias = col
                if self.current_token() and self.current_token()[0] == "AS":
                    self.eat("AS")
                    alias = self.eat(self.current_token()[0])[1]
                columns.append(Columns(col, alias))
                                
            elif self.current_token()[0] == "FUNC":
                func = self.parse_special_columns()
                function_columns.append(func)
            else:
                raise SyntaxError(f"Expected column name or function, got {self.current_token()[0]}")

            if self.current_token() and self.current_token()[0] == "COMMA":
                self.eat("COMMA")
            else:
                break
        # print(columns, function_columns)
        return columns, function_columns

    def parse_special_columns(self):
        is_unique = False
        if self.current_token() and self.current_token()[0] == "FUNC":
            func_name = self.eat("FUNC")[1]
            self.eat("OPEN_PAREN")
            if self.current_token() and self.current_token()[0] == "DISTINCT":
                self.eat("DISTINCT")
                is_unique = True
            if self.current_token() and self.current_token()[0] == "STAR":
                arg = self.eat("STAR")[1]
            else:
                arg = self.eat("IDENTIFIER")[1]
            self.eat("CLOSE_PAREN")
            if self.current_token() and self.current_token()[0] == "AS":
                self.eat("AS")
                if self.current_token()[0] == "STRING": col_alias = self.eat("STRING")[1]
                elif self.current_token()[0] == "IDENTIFIER": col_alias = self.eat("IDENTIFIER")[1]
                else:
                    raise SyntaxError(f"Invalid alias '{self.current_token()[1]}'. Aliases must be identifiers or strings.")
            else:
                col_alias = f"{func_name}({arg})"
            return FunctionCall(func_name, arg, alias=col_alias, distinct=is_unique)
            

    def group_by(self):
        self.eat("GROUP_BY_KEY")
        self.eat("GROUP_BY_KEY")
        group = []
        while True:
            if self.current_token() and self.current_token()[0] == "IDENTIFIER":
                arg = self.eat("IDENTIFIER")[1]
                group.append(arg)
            if self.current_token() and self.current_token()[0] == "COMMA":
                self.eat("COMMA")
                continue
            else:
                break
        # if not set(columns).issubset(group):
        #     raise ValueError(f"all selected columns must appear in the GROUP BY clause or be used in an aggregate function")
        return group
            
        

    def order_by(self):
        self.eat("ORDER_BY_KEY")
        self.eat("ORDER_BY_KEY")
        order = []
        while True:
            order_col = self.eat("IDENTIFIER")[1]
            if self.current_token() and self.current_token()[0] != "ORDER_BY_DRC":
                order_direction = "ASC"
            else:
                order_direction = self.eat("ORDER_BY_DRC")[1]
            order.append((order_col, order_direction))
            if self.current_token() and self.current_token()[0] == "COMMA":
                self.eat("COMMA")
            else:
                break
        return order
    
    def parse_having_expression(self):
        
        if self.current_token() and self.current_token()[0] == "FUNC":
            node = InpType(type="FUNC", content = self.parse_special_columns())
            
        elif self.current_token() and self.current_token()[0] == "IDENTIFIER":
            col = self.eat("IDENTIFIER")[1]
            node = InpType(type="ID", content=col)
            
        elif self.current_token() and self.current_token()[0] in ("STRING", "NUMBER", "BOOLEAN"):
            val = self.eat(self.current_token()[0])[1]
            node = InpType(type = "VALUE", content = val)
            
        else:
            raise ValueError("Second argument must be an aggregate function, a value, or a column from the GROUP BY clause.")
        
        return node
            
        
        
    def parse_having(self):
        if self.current_token()[1] == "(":
            self.eat("OPEN_PAREN")
            left_node = self.parse_having()
            self.eat("CLOSE_PAREN")
        else:
            left = self.parse_having_expression()
            low_operator = self.eat("LOW_PRIORITY_OPERATOR")[1]
            right = self.parse_having_expression()
            left_node = HavingCondition(left, low_operator, right)
                
        if self.current_token() and self.current_token()[0] == "HIGH_PRIORITY_OPERATOR":
            high_operator = self.eat("HIGH_PRIORITY_OPERATOR")[1]
            right_node = self.parse_having()
            return HavingLogicalCondition(left_node, high_operator, right_node)
        else:
            return left_node


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
        self.eat("OPEN_PAREN")
        columns = []
        while self.current_token() and  (self.current_token()[0] == "IDENTIFIER" or self.current_token()[0] == "COMMA"):
            if self.current_token()[0] != 'COMMA':
                columns.append(self.eat("IDENTIFIER")[1])
            else:
                self.eat("COMMA")
        self.eat("CLOSE_PAREN")
        return columns
    
    
    def parse_insert_values(self):
        self.eat("OPEN_PAREN")  # (
        values = []

        while self.current_token() and self.current_token()[0] != "CLOSE_PAREN":
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

        self.eat("CLOSE_PAREN")  # )
        return values

    def parse_condition_tree(self):
        
        if self.current_token()[1] == "(":
            self.eat("OPEN_PAREN")
            left_node = self.parse_condition_tree()
            self.eat("CLOSE_PAREN")
            
        if self.current_token()[0] == "IDENTIFIER":
            col = self.eat("IDENTIFIER")[1]  
        not_in_Membership = False
        not_in_between = False
        not_in_like = False
        
        
        if self.current_token()[0] == "NULLCHECK":
            
            self.eat("NULLCHECK")
            if self.current_token()[0] == "NULLCHECK":
                if self.current_token()[1].upper() != "NOT":
                    raise ValueError("Invalid Keyword for this Operation")
                self.eat("NULLCHECK")
                left_node = CheckNullColumn(col, isNull=False)
            else:
                left_node = CheckNullColumn(col, isNull=True)
            self.eat("NULL")
            
                    
        elif self.current_token()[0] == "MEMBERSHIP":
            args = set()
            not_in_membership = False  # Default to IN, not NOT IN
            
            self.eat("MEMBERSHIP")  # Consume first MEMBERSHIP token (likely "NOT" or "IN")
            
            # Check if this is a NOT IN scenario
            if self.current_token() and self.current_token()[0] == "MEMBERSHIP":
                if self.current_token()[1] == "IN":
                    self.eat("MEMBERSHIP")  # Consume the "IN" token
                    not_in_membership = True  # This was "NOT IN"
                else:
                    # Handle unexpected token after first MEMBERSHIP
                    raise ValueError(f"Expected 'IN' after 'NOT', but got '{self.current_token()[1]}'")
            # If no second MEMBERSHIP token, then this is just "IN"
            
            self.eat("OPEN_PAREN")  # Consume opening parenthesis
            
            # Parse the values inside parentheses
            while self.current_token() and self.current_token()[0] != "CLOSE_PAREN":
                token_type, token_value = self.current_token()
                
                if token_type in ("STRING", "BOOLEAN", "NUMBER"):
                    self.eat(token_type)
                    # Normalize string values to lowercase for consistency
                    if isinstance(token_value, str):
                        token_value = token_value.lower()
                    args.add(token_value)
                    
                    # Check for comma (LOW_PRIORITY_OPERATORional for last element)
                    if self.current_token() and self.current_token()[0] == "COMMA":
                        self.eat("COMMA")
                    elif self.current_token() and self.current_token()[0] != "CLOSE_PAREN":
                        raise ValueError(f"Expected ',' or ')' after value, but got '{self.current_token()[1]}'")
                        
                elif token_type == "COMMA":
                    # Handle case where there are consecutive commas or leading comma
                    raise ValueError("Unexpected comma in membership list")
                else:
                    raise ValueError(f"Invalid token '{token_value}' in membership list")
            
            if not self.current_token() or self.current_token()[0] != "CLOSE_PAREN":
                raise ValueError("Expected closing parenthesis ')' for membership list")
            
            self.eat("CLOSE_PAREN")  # Consume closing parenthesis
            
            if not args:
                raise ValueError("Empty membership list is not allowed")
            
            # Create the membership node with correct IN/NOT IN logic
            left_node = Membership(col, args, IN=not not_in_membership)
        

        elif self.current_token()[0] =="NegationCondition":
            self.eat("NegationCondition")
            self.eat("OPEN_PAREN")
            col = self.eat("IDENTIFIER")[1]
            op = self.eat("LOW_PRIORITY_OPERATOR")[1]
            if self.current_token()[0] in ("STRING", "BOOLEAN", "NUMBER"):
                token, token_val = self.current_token()
                val = token_val
                self.eat(token)
                left_node = NegationCondition(Condition(col, op, val))
            self.eat("CLOSE_PAREN")

        elif self.current_token()[0] == "BETWEEN":
            arg1 = None
            arg2 = None
            self.eat("BETWEEN")
            if self.current_token()[0] == "BETWEEN":
                if self.current_token()[1] != "BETWEEN":
                    raise ValueError(f"Expected 'BETWEEN' after 'NOT', but got '{self.current_token()[1]}'")
                else:
                    self.eat("BETWEEN")
                    not_in_between = True
            if self.current_token()[0] in ("STRING", "NUMBER"):
                arg1 = self.eat(self.current_token()[0])[1]
                if self.current_token()[1] != "AND":
                    raise ValueError("Missed 'AND' Operator in BETWEEN Comparison")
                self.eat("HIGH_PRIORITY_OPERATOR")
                arg2 = self.eat(self.current_token()[0])[1]
                if type(arg1) != type(arg2):
                    raise ValueError(
                    f"Type mismatch: '{arg1}' (type {type(arg1).__name__}) cannot be compared with '{arg2}' (type {type(arg2).__name__}). Please use values of the same data type for BETWEEN."
                        )
                else:
                    left_node = BetweenCondition(col, arg1, arg2, NOT=not_in_between)
            else:
                raise ValueError("Unsupported Data type for BETWEEN Comparison")
      
        elif self.current_token()[0] == "LIKE":
            self.eat("LIKE")
            if self.current_token()[0] == "LIKE":
                if self.current_token()[1] != "LIKE":
                    raise ValueError(f"Expected 'LIKE' after 'NOT', but got '{self.current_token()[1]}'")
                else:
                    self.eat("LIKE")
                    not_in_like = True
            arg = self.eat(self.current_token()[0])[1]
            left_node = LikeCondition(col, arg, NOT = not_in_like)
                     
        elif self.current_token()[0] == "LOW_PRIORITY_OPERATOR":
            op = self.eat("LOW_PRIORITY_OPERATOR")[1]
            if self.current_token()[0] in ("STRING", "NUMBER", "BOOLEAN"):
                token,  value = self.current_token()
                self.eat(token)
                val = value
                left_node = Condition(col, op, val)
            else:
                raise ValueError (f"Expected Valid Datatype for Where Clause but got {self.current_token()[1]}")
        
        # --- Check for logical operator (AND/OR) ---
        if self.current_token() and self.current_token()[0] == "HIGH_PRIORITY_OPERATOR":
            operator = self.eat("HIGH_PRIORITY_OPERATOR")[1]
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
            LOW_PRIORITY_OPERATOR = self.eat("LOW_PRIORITY_OPERATOR")[1]
            if LOW_PRIORITY_OPERATOR not in ("=", "=="):
                raise SyntaxError(f"Invalid assignment operator '{LOW_PRIORITY_OPERATOR}' for column '{col}'. Use '=' or '=='.")

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
        self.eat("OPEN_PAREN")  # (
        schema = {}
        auto = {}
        defaults = {} 
        is_serial = False
        while self.current_token() and self.current_token()[0] != "CLOSE_PAREN":
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
                LOW_PRIORITY_OPERATOR = self.eat("LOW_PRIORITY_OPERATOR")[1]
                if LOW_PRIORITY_OPERATOR not in ("=", "=="):
                    raise ValueError("Syntax error in DEFAULT clause: expected '=' or '==' after DEFAULT keyword")
                token_type, token_value = self.current_token()
                if token_type:
                    default_value = self.eat(token_type)[1]
                    defaults[col_name] = schema[col_name](default_value)
            # Skip comma if present
            if self.current_token() and self.current_token()[0] == "COMMA":
                self.eat("COMMA")
            print(col_name)

        self.eat("CLOSE_PAREN")  # )
        self.eat("SEMICOLON")
        return CreateTableStatement(table_name, schema, defaults, auto)
        


    def parse_use_statement(self):
        self.eat("USE")
        db_name = self.eat("IDENTIFIER")[1]
        self.eat("SEMICOLON")
        return UseStatement(db_name)


    def parse_expression(self):
        """Parse mathematical expressions with operator precedence"""
        return self.parse_addition()



    def parse_addition(self):
        left = self.parse_multiplication()
        
        while self.current_token() and self.current_token()[1] in ['+', '-']:
            operator = self.current_token()[1]
            self.eat('MATH_OPERATOR')  # or whatever token type
            right = self.parse_multiplication()
            left = BinaryOperation(left, operator, right)
        
        return left


    def parse_multiplication(self):
        left = self.parse_factor()
        
        while self.current_token() and self.current_token()[1] in ['*', '/']:
            operator = self.current_token()[1]
            self.eat('MATH_OPERATOR')
            right = self.parse_factor()
            left = BinaryOperation(left, operator, right)
        
        return left


    def parse_factor(self):
        token = self.current_token()
        
        if token[0] == 'LPAREN':  # (
            self.eat('LPAREN')
            expr = self.parse_expression()  # Recursively parse inside parentheses
            self.eat('RPAREN')
            return expr
        elif token[0] == 'IDENTIFIER':
            self.eat('IDENTIFIER')
            return ColumnExpression(token[1])
        elif token[0] == 'NUMBER':
            self.eat('NUMBER')
            return LiteralExpression(token[1])
        else:
            raise ValueError(f"Unexpected token in expression: {token}")