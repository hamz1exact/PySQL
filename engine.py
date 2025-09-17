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
    concat = {
        "CONCAT"
    }
    
    between = {
        "BETWEEN"
    }
    
    cast =  {
        "CAST"
    }
    
    date_and_time = {
        "YEAR",
        "MONTH",
        "DAY",
        "HOUR",
        "MINUTE",
        "SECOND",
        "CURRENT_DATE",
        "NOW",
        "DATEDIFF"
    }
    
    
    extract = {
        "EXTRACT"
    }
    
    nullif = {
        "NULLIF"
    }
    
    replace = {
        "REPLACE"
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
    
    coalesce = {
        "COALESCE"
    }
    
    math_functions = {
        
        "ABS",
        "ROUND",
        "CEIL",
        "FLOOR"
    }
    
    string_functions = {
        "UPPER",
        "LOWER",
        "LENGTH",
        "SUBSTRING",
        "REVERSE"
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
                    
                elif upper_word in Lexer.date_and_time:
                    self.tokens.append(("DATE_AND_TIME", upper_word)) 
                elif upper_word in Lexer.limit_keys:
                    self.tokens.append(("LIMIT", "LIMIT"))
                    
                elif upper_word in Lexer.coalesce:
                    self.tokens.append(("COALESCE", "COALESCE"))
                    
                elif upper_word in Lexer.concat:
                    self.tokens.append(("CONCAT", "CONCAT"))
                    
                elif upper_word in Lexer.replace:
                    self.tokens.append(("REPLACE", "REPLACE"))

                elif upper_word in Lexer.string_functions:
                    self.tokens.append(("STRING_FUNC", upper_word))
                    
                
                elif upper_word in Lexer.offset_keys:
                    self.tokens.append(("OFFSET", "OFFSET"))
                    
                elif upper_word in Lexer.math_functions:
                    self.tokens.append(("MATH_FUNC", upper_word))
                    
                elif upper_word in Lexer.nullif:
                    self.tokens.append(("NULLIF", "NULLIF"))
                    
                elif upper_word in Lexer.extract:
                    self.tokens.append(("EXTRACT", "EXTRACT")) 
                    
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
                    
                elif upper_word in Lexer.cast:
                    self.tokens.append(("CAST", "CAST"))
                    
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
    
    _AGG_FUNCS = {"COUNT", "SUM", "AVG", "MIN", "MAX"}
    
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
            where = self.parse_expression(context = "WHERE")
            
            
        if self.current_token() and self.current_token()[0] == "GROUP_BY_KEY":
            group_in = self.group_by()
            if self.current_token() and self.current_token()[0] == "HAVING":
                self.eat("HAVING")
                having_in = self.parse_expression(context = "HAVING")
                
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
        alias_name = None
        while True:
            expr = self.parse_addition(context=None)
            
            if self.current_token() and self.current_token()[0] == "AS":
                self.eat("AS")
                alias_name = (self.eat(self.current_token()[0])[1]).lower()
                # Attach alias to expr
                if isinstance(expr, ColumnExpression) or isinstance(expr, BinaryOperation) or isinstance(expr, Function) or isinstance(expr, MathFunction) or isinstance(expr, StringFunction) or isinstance(expr, Replace) or isinstance(expr, Concat) or isinstance(expr, Cast) or isinstance(expr, CoalesceFunction) or isinstance(expr, Extract) or isinstance(expr, CurrentDate) or isinstance(expr, DateDIFF):
                    expr.alias = alias_name
            if self._contains_aggregates(expr):
                function_columns.append(expr)
            else:
                columns.append(expr)
            
            if self.current_token() and self.current_token()[0] == "COMMA":
                self.eat("COMMA")
            else:
                break
        
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
            arg = self.parse_expression()
            group.append(arg)
            if self.current_token() and self.current_token()[0] == "COMMA":
                self.eat("COMMA")
                continue
            else:
                break

        return group
            
        

    def order_by(self):
        self.eat("ORDER_BY_KEY")
        self.eat("ORDER_BY_KEY")
        order = []
        while True:
            expression = self.parse_expression(None)
            if self.current_token() and self.current_token()[0] != "ORDER_BY_DRC":
                order_direction = "ASC"
            else:
                order_direction = self.eat("ORDER_BY_DRC")[1]
            order.append(OrderBy(expression,order_direction))
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

    def parse_expression(self, context = None):
        """Parse mathematical expressions with operator precedence"""
        
        return self.parse_logical_condition(context)
    

    def parse_logical_condition(self, context):
        left = self.parse_condition_engine(context)
        while self.current_token() and self.current_token()[1] in ["AND", "OR"]:
            operator = self.eat(self.current_token()[0])[1]
            right = self.parse_condition_engine(context)
            
            if context == "WHERE":
                left = ConditionExpr(left, operator, right, context = "WHERE")
                self.validate_no_aggregate_in_where(left)                
            else:
                left = ConditionExpr(left, operator, right, context = "HAVING")
        return left
    
    def parse_condition_engine(self, context):
        expression = self.parse_condition(context)
        while self.current_token() and self.current_token()[0] == "LIKE":
            is_not = False
            if self.current_token()[1] == "NOT":
                is_not = True
                self.eat("LIKE")
            self.eat("LIKE")
            arg = LiteralExpression(self.eat("STRING")[1])
            return LikeCondition(expression, arg, is_not)
        while self.current_token() and self.current_token()[0] == "NULLCHECK":
            is_null = True
            self.eat("NULLCHECK")
            if self.current_token() and self.current_token()[0] == "NULLCHECK":
                self.eat("NULLCHECK")
                is_null = False
            self.eat("NULL")
            return IsNullCondition(expression, is_null=is_null)
        while self.current_token() and self.current_token()[0] == "MEMBERSHIP":
            args = []
            is_nott = False
            self.eat("MEMBERSHIP")
            if self.current_token()[0] == "MEMBERSHIP":
                self.eat("MEMBERSHIP")
                is_nott = True
            self.eat("OPEN_PAREN")
            while True:
                arg = self.parse_factor(context)
                args.append(arg)
                if self.current_token() and self.current_token()[0] == "COMMA":
                    self.eat("COMMA")
                else:
                    self.eat("CLOSE_PAREN")
                    break
            return Membership(expression, args, is_not=is_nott)
        while self.current_token() and self.current_token()[0] == "BETWEEN":
            is_nott = False
            if self.current_token()[1] == "NOT":
                self.eat("BETWEEN")
                is_nott = True
            if self.current_token() and self.current_token()[0] == "BETWEEN":
                self.eat("BETWEEN")
            lower = self.parse_factor(context)
            self.eat("HIGH_PRIORITY_OPERATOR")
            upper = self.parse_factor(context)
            return Between(expression, lower, upper, is_not = is_nott)
        
        return expression
    
    def parse_condition(self, context):
        left = self.parse_addition(context)

        while self.current_token() and self.current_token()[1] in ["=", "!=", ">", "<", ">=", "<="]:
            operator = self.eat(self.current_token()[0])[1]
            right = self.parse_addition(context)
            
            if context == "WHERE":
                self.validate_no_aggregate_in_where(left)
                left = ConditionExpr(left, operator, right, context="WHERE")
            else:
                left = ConditionExpr(left, operator, right, context = "HAVING")
        return left


    def parse_addition(self, context):
        left = self.parse_multiplication(context)
        
        while self.current_token() and self.current_token()[1] in ['+', '-']:
            
            operator = self.current_token()[1]
            self.eat('MATH_OPERATOR')  # or whatever token type
            right = self.parse_multiplication(context)
            left = BinaryOperation(left, operator, right)
        
        return left


    def parse_multiplication(self, context):
        
        left = self.parse_factor(context)
        
        while self.current_token() and self.current_token()[1] in ['*', '/']:
            
            operator = self.current_token()[1]
            self.eat(self.current_token()[0])
            
            right = self.parse_factor(context)
            left = BinaryOperation(left, operator, right)
        
        return left
    
    
    
    def parse_factor(self, context = None):
        
        token = self.current_token()
        
        if token[0] == 'OPEN_PAREN':  # (
            self.eat('OPEN_PAREN')
            expr = self.parse_expression(context)  # Recursively parse inside parentheses
            self.eat('CLOSE_PAREN')
            return expr
        
        elif token[0] == 'IDENTIFIER':
            self.eat("IDENTIFIER")    
            return ColumnExpression(token[1])
        
        elif token[0] == "MATH_FUNC":
            name = self.eat("MATH_FUNC")[1]
            round_by = None
            self.eat("OPEN_PAREN")
            expression = self.parse_expression(context)
            if self.current_token()[0] == "COMMA":
                self.eat("COMMA")
                round_by = int(self.eat("NUMBER")[1])
            self.eat("CLOSE_PAREN")
            return MathFunction(name, expression, round_by)
            
        elif token[0] == "STRING_FUNC":
            name = self.eat("STRING_FUNC")[1]
            start = length = None
            self.eat("OPEN_PAREN")
            expression = self.parse_expression(context)
            if self.current_token()[0] == "COMMA":
                self.eat("COMMA")
                start = int(self.eat("NUMBER")[1])
                if self.current_token()[0] == "COMMA":
                    self.eat("COMMA")
                    length = int(self.eat("NUMBER")[1])
            self.eat("CLOSE_PAREN")
            return StringFunction(name, expression, start, length)
        
        elif token[0] == "REPLACE":
            self.eat("REPLACE")
            self.eat("OPEN_PAREN")
            expression = self.parse_expression(context)
            self.eat("COMMA")
            old = self.parse_expression(context)
            self.eat("COMMA")
            new = self.parse_expression(context)
            self.eat("CLOSE_PAREN")
            return Replace(expression, old, new)
        
        elif token[0] == "CONCAT":
            self.eat("CONCAT")
            expressions = []
            self.eat("OPEN_PAREN")
            while self.current_token()[0] != "CLOSE_PAREN":
                arg = self.parse_expression()
                expressions.append(arg)
                
                if self.current_token()[0] == "COMMA":
                    self.eat("COMMA")
            self.eat("CLOSE_PAREN")
            return Concat(expressions)
        
            
        elif token[0] == "DATE_AND_TIME" and token[1] == "DATEDIFF":
            unit = 'days'    
            self.eat("DATE_AND_TIME")
            self.eat("OPEN_PAREN")
            date1 = self.parse_expression()
            self.eat("COMMA")
            date2 = self.parse_expression()
            if self.current_token()[0] == "COMMA":
                self.eat("COMMA")
                unit = self.parse_expression()
                if not isinstance(unit, LiteralExpression):
                    raise ValueError("unit must be represented as string")
            self.eat("CLOSE_PAREN")
            return DateDIFF(date1, date2, unit)
        
        
        elif token[0] == "EXTRACT":
            self.eat("EXTRACT")
            self.eat("OPEN_PAREN")
            part = self.eat("DATE_AND_TIME")[1].upper()
            self.eat("FROM")
            expression = self.parse_expression()
            self.eat("CLOSE_PAREN")
            return Extract(expression, part)
            

            
        
        elif token[0] == "COALESCE":
            expressions = []
            self.eat("COALESCE")
            self.eat("OPEN_PAREN")
            while self.current_token() and self.current_token()[0] != "CLOSE_PAREN":
                exp = self.parse_expression()
                expressions.append(exp)
                
                if self.current_token()[0] == "COMMA":
                    self.eat("COMMA")
                
            self.eat("CLOSE_PAREN")
            return CoalesceFunction(expressions)
        
        elif token[0] == "NULLIF":
            self.eat("NULLIF")
            self.eat("OPEN_PAREN")
            expr = self.parse_expression()
            self.eat("COMMA")
            number = self.parse_expression()
            self.eat("CLOSE_PAREN")
            return NullIF(expr, number)
                
        elif token[0] == "CAST":
            self.eat("CAST")
            self.eat("OPEN_PAREN")
            expression = self.parse_expression()
            if self.current_token()[0] in ("COMMA", "AS"):
                self.eat(self.current_token()[0])
            target = self.eat(self.current_token()[0])[1].upper()
            if target not in Lexer.datatypes or target == "SERIAL":
                raise ValueError("Invalid Given Data Type")
            self.eat("CLOSE_PAREN")
            return Cast(expression, target)
            
        elif token[0] == "DATE_AND_TIME" and token[1] == "CURRENT_DATE":
            self.eat("DATE_AND_TIME")
            return CurrentDate()

        
        elif token[0] == "FUNC":
            distinct = False
            name = self.eat("FUNC")[1]
            self.eat("OPEN_PAREN")
            if self.current_token() and self.current_token()[0] == "DISTINCT":
                self.eat("DISTINCT")
                distinct = True
            expression = self.parse_expression(context)
            self.eat("CLOSE_PAREN")
            return Function(name, expression, distinct=distinct)
        
        elif token[0] == 'NUMBER' or token[0] == "BOOLEAN" or token[0] == "STRING":
            self.eat(self.current_token()[0])
            return LiteralExpression(token[1])
        
        elif token[0] == "STAR":
            self.eat("STAR")
            return ColumnExpression(token[1])
        
        elif token[0] == "NOT":
            self.eat("NOT")
            expr = self.parse_expression(context)
            
            return NegationCondition(expr)
        
        else:
            raise ValueError(f"Unexpected token in expression: {token}")
        
        
    def _contains_aggregates(self, expr):
        """Check if an expression contains aggregate functions"""
        if isinstance(expr, Function): 
            return True
        elif isinstance(expr, MathFunction) or isinstance(expr, StringFunction) or isinstance(expr, Replace) or isinstance(expr, Cast) or isinstance(expr, NullIF):
            
            return self._contains_aggregates(expr.expression)
        elif isinstance(expr, Concat) or isinstance(expr, CoalesceFunction):
            for sub_expr in expr.expressions:
                if self._contains_aggregates(sub_expr):
                    return True
            return False
        elif isinstance(expr, CurrentDate):
            return False
        elif isinstance(expr, DateDIFF):
            if self._contains_aggregates(expr.date1) or self._contains_aggregates(expr.date2):
                return True
            return False
        elif isinstance(expr, Extract):  # THIS WAS MISSING!
            return self._contains_aggregates(expr.expression)  
        
        elif isinstance(expr, BinaryOperation):
            # Check both sides of the operation
            return (self._contains_aggregates(expr.left) or 
                    self._contains_aggregates(expr.right))
        elif isinstance(expr, ColumnExpression):
            return False
        elif isinstance(expr, LiteralExpression):
            return False
        else:

            return False
                    
    def _has_aggregation_in_expr(self, expr) -> bool:
        if expr is None:
            return False

        if isinstance(expr, Function):
            func_name = getattr(expr, "name", None) or getattr(expr, "function_name", None)
            if func_name and func_name.upper() in Parser._AGG_FUNCS:
                return True
            if hasattr(expr, "expression") and expr.expression is not None:
                if self._has_aggregation_in_expr(expr.expression):
                    return True
            if hasattr(expr, "arg") and expr.arg is not None:
                if self._has_aggregation_in_expr(expr.arg):
                    return True
            if hasattr(expr, "args") and expr.args:
                for a in expr.args:
                    if self._has_aggregation_in_expr(a):
                        return True
            return False

        if isinstance(expr, ConditionExpr):
            return self._has_aggregation_in_expr(expr.left) or self._has_aggregation_in_expr(expr.right)

            
        
        if isinstance(expr, BinaryOperation):
            return self._has_aggregation_in_expr(expr.left) or self._has_aggregation_in_expr(expr.right)

        if isinstance(expr, (ColumnExpression, LiteralExpression)):
            return False

        if isinstance(expr, (list, tuple, set)):
            for item in expr:
                if self._has_aggregation_in_expr(item):
                    return True
            return False
        for attr in ("left", "right", "expression", "expr", "args", "arg"):
            child = getattr(expr, attr, None)
            if child is None:
                continue
            if self._has_aggregation_in_expr(child):
                return True

        return False
    
    

    
    def validate_no_aggregate_in_where(self, where_expr):
        if self._has_aggregation_in_expr(where_expr):
            raise ValueError("Aggregation functions (COUNT, SUM, AVG, MIN, MAX) are not allowed in WHERE — use HAVING instead.")