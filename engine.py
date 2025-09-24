from sql_ast import *
from executor import execute 
from datetime import datetime
from utilities import db_manager
from datatypes import * 
from constants import *




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
        # Parse the full SELECT statement first
        stmt = self.parse_single_select()
        
        # Now check for UNION/INTERSECT at this level
        while self.current_token() and self.current_token()[0] in Lexer.set_operators:
            if self.current_token()[0] == TokenTypes.UNION:
                self.eat(TokenTypes.UNION)
                union_type = TokenTypes.UNION
                if self.current_token() and self.current_token()[0] == TokenTypes.ALL:
                    self.eat(TokenTypes.ALL)
                    union_type = TokenTypes.UNION_ALL
                
                # Parse the next SELECT statement
                right_stmt = self.parse_single_select()
                stmt = UnionExpression(stmt, right_stmt, union_type)
                
            elif self.current_token()[0] == TokenTypes.INTERSECT:
                self.eat(TokenTypes.INTERSECT)
                right_stmt = self.parse_single_select()
                stmt = IntersectExpression(stmt, right_stmt)
            
            elif self.current_token()[0] == TokenTypes.EXCEPT:
                self.eat(TokenTypes.EXCEPT)
                right_stmt = self.parse_single_select()
                stmt = ExceptExpression(stmt, right_stmt)
            
        return stmt
    
    def parse_single_select(self):
        """Parse a single SELECT statement without UNION/INTERSECT"""
        self._requested_pointer = None
        self._pointers = {}
        where = None
        unique = False
        order_in = []
        group_in = []
        having_in = None
        limit, offset = None, None
        table_ref = None
        
        self.eat(TokenTypes.SELECT)
        if self.current_token() and self.current_token()[0] == TokenTypes.DISTINCT:
            self.eat(TokenTypes.DISTINCT)
            unique = True
        columns, function_columns = self.parse_columns()
        
        if self.current_token() and self.current_token()[0] == TokenTypes.FROM:
            self.eat(TokenTypes.FROM)
            table_ref = self.parse_table()
            if table_ref.alias:
                if self._requested_pointer is not None and table_ref.alias != self._requested_pointer:
                    raise ValueError(f"""invalid reference to FROM-clause entry for table "{table_ref.table_name}"\nPerhaps you meant to reference the table alias '{table_ref.alias}'.""")
                self._pointers[table_ref.alias] = table_ref.table_name
            elif table_ref.alias is None and self._requested_pointer is not None:
                raise ValueError(f""" missing FROM-clause alias for table {table_ref.table_name} """)
        
        if self.current_token() and self.current_token()[0] == TokenTypes.WHERE:
            self.eat(TokenTypes.WHERE)
            where = self.parse_expression(context=TokenTypes.WHERE)
        
        if self.current_token() and self.current_token()[0] == TokenTypes.GROUP_BY:
            group_in = self.group_by()
            if self.current_token() and self.current_token()[0] == TokenTypes.HAVING:
                self.eat(TokenTypes.HAVING)
                having_in = self.parse_expression(context=TokenTypes.HAVING)
        
        if self.current_token() and self.current_token()[0] == TokenTypes.ORDER_BY:
            order_in = self.order_by()
        
        if self.current_token() and self.current_token()[0] == TokenTypes.OFFSET:
            raise ValueError("The OFFSET clause must be used with a LIMIT clause.")
        
        if self.current_token() and self.current_token()[0] == TokenTypes.LIMIT:
            limit = self.eat(TokenTypes.LIMIT)[1]
            if self.current_token() and self.current_token()[0] == TokenTypes.OFFSET:
                offset = self.eat(TokenTypes.OFFSET)[1]

        return SelectStatement(columns, function_columns, table_ref, where, 
                             distinct=unique, order_by=order_in, group_by=group_in, 
                             having=having_in, limit=limit, offset=offset)
                  
    def parse_columns(self):
        columns = []
        function_columns = []
        alias_name = None
        self._select_aliases = {}
        while True:
            expr = self.parse_addition(context=None)
            if self.current_token() and self.current_token()[0] == TokenTypes.AS:
                self.eat(TokenTypes.AS)
                alias_name = (self.eat(self.current_token()[0])[1]).lower()
                self._select_aliases[alias_name] = expr
                # Attach alias to expr
                if isinstance(expr, (ColumnExpression, BinaryOperation, Function, MathFunction, StringFunction, Replace, Concat, Cast, CoalesceFunction, Extract, CurrentDate, DateDIFF, CaseWhen, LiteralExpression)):
                    expr.alias = alias_name
            if self._contains_aggregates(expr):
                function_columns.append(expr)
            else:
                columns.append(expr)
            
            if self.current_token() and self.current_token()[0] == TokenTypes.COMMA:
                self.eat(TokenTypes.COMMA)
            else:
                break
        
        return columns, function_columns
    
    
    def parse_table(self):
        """Parse table reference which can be a table name or subquery"""
        if self.current_token()[0] == TokenTypes.IDENTIFIER:
            table_name = self.eat(TokenTypes.IDENTIFIER)[1]
            table_ref = TableReference(table_name)
        elif self.current_token()[0] == TokenTypes.OPEN_PAREN:
            # This is a subquery in FROM clause
            self.eat(TokenTypes.OPEN_PAREN)
            subquery = self.parse_select_statement()  # Use full SELECT parser for nested queries
            self.eat(TokenTypes.CLOSE_PAREN)
            table_ref = TableReference(subquery)
        else:
            raise SyntaxError(f"Expected table name or subquery, got {self.current_token()}")
        
        # Handle alias
        alias = None
        if self.current_token() and self.current_token()[0] == TokenTypes.AS:
            self.eat(TokenTypes.AS)
            alias = self.eat(TokenTypes.IDENTIFIER)[1]
        elif self.current_token() and self.current_token()[0] == TokenTypes.IDENTIFIER:
            alias = self.eat(TokenTypes.IDENTIFIER)[1]
        
        table_ref.alias = alias
        return table_ref


    def group_by(self):
        self.eat(TokenTypes.GROUP_BY)  # GROUP
        self.eat(TokenTypes.GROUP_BY)  # BY
        group = []
        
        while True:
            # Try to parse as expression first
            if self.current_token() and self.current_token()[0] == TokenTypes.IDENTIFIER:
                identifier = self.current_token()[1]
                
                # Check if this is an alias from SELECT clause
                if identifier.lower() in self._select_aliases:
                    # This is an alias - use the original expression
                    self.eat(TokenTypes.IDENTIFIER)  # consume the identifier
                    resolved_expr = self._select_aliases[identifier.lower()]
                    group.append(resolved_expr)
                else:
                    # Regular column or expression
                    expr = self.parse_expression()
                    group.append(expr)
            else:
                # Complex expression
                expr = self.parse_expression()
                group.append(expr)
            
            if self.current_token() and self.current_token()[0] == TokenTypes.COMMA:
                self.eat(TokenTypes.COMMA)
                continue
            else:
                break

        return group
            
        

    def order_by(self):
        self.eat(TokenTypes.ORDER_BY)  # ORDER
        self.eat(TokenTypes.ORDER_BY)  # BY
        
        order = []
        
        while True:
            # Check for alias resolution
            if self.current_token() and self.current_token()[0] == TokenTypes.IDENTIFIER:
                identifier = self.current_token()[1]
                
                if identifier.lower() in self._select_aliases:
                    # This is an alias - use the original expression
                    self.eat(TokenTypes.IDENTIFIER)  # consume the identifier
                    expression = self._select_aliases[identifier.lower()]
                else:
                    # Regular column or expression
                    expression = self.parse_expression(None)
            else:
                # Complex expression
                expression = self.parse_expression(None)
            
            if self.current_token() and self.current_token()[1] not in Lexer.order_by_drc:
                order_direction = TokenTypes.ASCENDING_ORDER
            else:
                order_direction = self.eat("ORDER_BY_DRC")[1]
                
            order.append(OrderBy(expression, order_direction))
            
            if self.current_token() and self.current_token()[0] == TokenTypes.COMMA:
                self.eat(TokenTypes.COMMA)
            else:
                break
        return order
    
        
    def parse_request_statement(self):
        self.eat(TokenTypes.SHOW)
        if self.current_token() and self.current_token()[0] == TokenTypes.VIEWS:
            self.parse_list_viws()
            self.eat(TokenTypes.SEMICOLON)
        else:
            col = None
            if self.current_token()[1] != TokenTypes.CONSTRAINTS:
                raise ValueError("SHOW Must be followed by CONSTRAINTS")
            self.eat(TokenTypes.IDENTIFIER)
            if self.current_token()[0] == TokenTypes.OPEN_PAREN:
                self.eat(TokenTypes.OPEN_PAREN)
                table_name = self.eat(self.current_token()[0])[1]
                if self.current_token() and self.current_token()[0] == TokenTypes.DOT:
                    self.eat(TokenTypes.DOT)
                    col = self.eat(TokenTypes.IDENTIFIER)[1]
                self.eat(TokenTypes.CLOSE_PAREN)
            elif self.current_token()[0] == TokenTypes.STAR:
                self.eat(TokenTypes.STAR)
                table_name = self.eat(self.current_token()[0])[1]
                if self.current_token() and self.current_token()[0] == TokenTypes.DOT:
                    self.eat(TokenTypes.DOT)
                    col = self.eat(TokenTypes.IDENTIFIER)[1]
            return ShowConstraints(table_name, col=col)
        


    def parse_insert_statement(self):
        conflict = False
        conflict_targets = None
        action = None
        update_cols = None
        returned_columns = None
        self.eat(TokenTypes.INSERT)
        self.eat(TokenTypes.INTO)
        table = self.parse_insert_table()
        insertion_data = self.parse_insert_cols_vals()
        if self.current_token() and self.current_token()[0] == TokenTypes.CONFLICT:
            self.eat(TokenTypes.CONFLICT) # On
            self.eat(TokenTypes.CONFLICT) # Conflict
            conflict = True # set Conflict to be True
            if self.current_token() and self.current_token()[0] == TokenTypes.OPEN_PAREN:
                self.eat(TokenTypes.OPEN_PAREN)
                conflict_targets = self.eat(TokenTypes.IDENTIFIER)[1]
                self.eat(TokenTypes.CLOSE_PAREN)
            self.eat(TokenTypes.CONFLICT)
            action = self.eat(TokenTypes.ACTION)[1]
            if action == TokenTypes.UPDATE:
                self.eat(TokenTypes.SET)
                update_cols = self.parse_update_columns()
            
        if self.current_token() and self.current_token()[0] == TokenTypes.RETURNING:
            returned_columns = ReturningClause(columns=self.parse_returning_columns(), table_name=table)
        self.eat(TokenTypes.SEMICOLON)
        return InsertStatement(table, insertion_data, conflict, conflict_targets, action, update_cols, returned_cols=returned_columns)
    
    
    def parse_insert_cols_vals(self):
        res = []
        cols = []
        if self.current_token()[0] == TokenTypes.OPEN_PAREN:
            cols = self.parse_insert_columns()
        if self.current_token()[0] == TokenTypes.VALUES:
            self.eat(TokenTypes.VALUES)
            while True: 
                vals = []
                self.eat(TokenTypes.OPEN_PAREN)
                while self.current_token() and self.current_token()[0] != TokenTypes.CLOSE_PAREN:
                    if self.current_token()[0] == TokenTypes.COMMA:
                        self.eat(TokenTypes.COMMA)
                        continue

                    # Accept type-aware values
                    token_type, token_value = self.current_token()
                    if token_type in ON_INSERT_UPDATE_ACCEPTED_DATA_TYPES:
                        val = self.eat(token_type)[1]
                    else:
                        raise SyntaxError(
                            f"Unexpected token type '{token_type}' in VALUES clause. Must be NUMBER, STRING, or BOOLEAN."
                        )
                    vals.append(val)
                
                res.append(InsertExpression(columns=cols, values=vals))
                self.eat(TokenTypes.CLOSE_PAREN)
                if self.current_token() and self.current_token()[0] == TokenTypes.COMMA:
                    self.eat(TokenTypes.COMMA)
                else:
                    break
        return res
                

    
    
    def parse_insert_table(self):
        return self.eat(TokenTypes.IDENTIFIER)[1]
    
    

    def parse_insert_columns(self):
        self.eat(TokenTypes.OPEN_PAREN)
        columns = []
        while self.current_token() and  (self.current_token()[0] == TokenTypes.IDENTIFIER or self.current_token()[0] == TokenTypes.COMMA):
            if self.current_token()[0] != TokenTypes.COMMA:
                columns.append(self.eat(TokenTypes.IDENTIFIER)[1])
            else:
                self.eat(TokenTypes.COMMA)
        self.eat(TokenTypes.CLOSE_PAREN)
        return columns
    
    
    def parse_insert_values(self):
        self.eat(TokenTypes.OPEN_PAREN)  # (
        values = []
        while self.current_token() and self.current_token()[0] != TokenTypes.CLOSE_PAREN:
            # Skip commas
            if self.current_token()[0] == TokenTypes.COMMA:
                self.eat(TokenTypes.COMMA)
                continue

            # Accept type-aware values
            token_type, token_value = self.current_token()
            if token_type in ON_INSERT_UPDATE_ACCEPTED_DATA_TYPES:
                val = self.eat(token_type)[1]
            else:
                raise SyntaxError(
                    f"Unexpected token type '{token_type}' in VALUES clause. Must be NUMBER, STRING, or BOOLEAN."
                )
            values.append(val)

        self.eat(TokenTypes.CLOSE_PAREN)  # )
        return values


    def parse_returning_columns(self):
        self.eat(TokenTypes.RETURNING)
        
        returned_columns, returned_function_columns = self.parse_columns()
        if returned_function_columns:
            raise ValueError('Aggregation Function is not allowed in RETURNING statement')
        return returned_columns

        
        
    def parse_update_statement(self):
        self.eat(TokenTypes.UPDATE)
        table_name = self.eat(TokenTypes.IDENTIFIER)[1]
        self.eat(TokenTypes.SET)
        columns_to_values = self.parse_update_columns()
        where = None
        returned_columns = None
        curr_token  = self.current_token()
        if curr_token and curr_token[0] == TokenTypes.WHERE:
            self.eat(TokenTypes.WHERE)
            where = self.parse_expression(context=TokenTypes.WHERE)
        if self.current_token() and self.current_token()[0] == TokenTypes.RETURNING:

            returned_columns = ReturningClause(self.parse_returning_columns(), table_name)
        self.eat(TokenTypes.SEMICOLON)
        
        return UpdateStatement(table_name, columns_to_values, where, returned_columns)

    def parse_update_columns(self):
        columns_to_values = {}

        while self.current_token() and self.current_token()[0] != TokenTypes.SEMICOLON and self.current_token()[0] not in Lexer.keywords:
            # Skip commas
            if self.current_token()[0] == TokenTypes.COMMA:
                self.eat(TokenTypes.COMMA)
                continue

            # Column name
            col = self.eat(TokenTypes.IDENTIFIER)[1]

            # Assignment operator
            LOW_PRIORITY_OPERATOR = self.eat(TokenTypes.LOW_PRIORITY_OPERATOR)[1]
            if LOW_PRIORITY_OPERATOR not in ("=", "=="):
                raise SyntaxError(f"Invalid assignment operator '{LOW_PRIORITY_OPERATOR}' for column '{col}'. Use '=' or '=='.")
            val = self.parse_addition(context= None)
            columns_to_values[col] = val

        return columns_to_values

    def parse_delete_statement(self):
        self.eat(TokenTypes.DELETE)
        self.eat(TokenTypes.FROM)
        table = self.eat(TokenTypes.IDENTIFIER)[1]
        token = self.current_token()
        where = None
        returned_columns = None
        if token and token[0] == TokenTypes.WHERE:
            self.eat(TokenTypes.WHERE)
            where = self.parse_expression(context=TokenTypes.WHERE)
        if self.current_token() and self.current_token()[0] == TokenTypes.RETURNING:
            returned_columns = ReturningClause(self.parse_returning_columns(), table_name=table)
        self.eat(TokenTypes.SEMICOLON)
        return DeleteStatement(table, where, returned_columns=returned_columns)
    
    
    def parse_create_database(self):
        self.eat(TokenTypes.CREATE)
        self.eat(TokenTypes.DATABASE)
        db_name = self.eat(TokenTypes.IDENTIFIER)[1]
        self.eat(TokenTypes.SEMICOLON)
        return CreateDatabseStatement(db_name)



    def parse_alter_table(self):
        self.eat(TokenTypes.ALTER)
        self.eat(TokenTypes.TABLE)
        expressions = []
        table_name = self.eat(TokenTypes.IDENTIFIER)[1]
        expr = self.parse_expression()
        expressions.append(expr)
        self.eat(TokenTypes.SEMICOLON)
        return AlterTable(table_name=table_name, expressions=expressions)
    
    def parse_create_table(self):
        self.eat(TokenTypes.CREATE)
        self.eat(TokenTypes.TABLE)
        table_name = self.eat(TokenTypes.IDENTIFIER)[1]
        if self.current_token()[0] == TokenTypes.AS:
            return self.parse_cta(table_name)
        
        table_name = table_name.strip()
        self.eat(TokenTypes.OPEN_PAREN)  # (
        schema = {}
        auto = {}
        defaults = {} 
        constraints = {}
        restrictions = {}
        private_constraints = {}
        constraints_ptr = {}
        
        is_serial = False
        is_default = False
        while self.current_token() and self.current_token()[0] != TokenTypes.CLOSE_PAREN:
            # Column name
            col_name = self.eat(TokenTypes.IDENTIFIER)[1]
            # Column type
            col_type = self.eat("DATATYPE")[1]
            if col_type in Lexer.datatypes:
                schema[col_name] = Lexer.datatypes[col_type]
            else:
                raise ValueError(f"Unknown Datatype -> {col_type}")
            # Handle SERIAL
            if col_type.upper() == TokenTypes.SERIAL:
                auto[col_name] = Lexer.datatypes[TokenTypes.SERIAL]()
                is_serial = True
            else:
                is_serial = False
                     

            # Handle DEFAULT values
            if self.current_token() and self.current_token()[0] == TokenTypes.DEFAULT:
                if is_serial:
                    raise ValueError(f"Invalid DEFAULT for column '{col_name}', SERIAL columns cannot have explicit default values.")
                
                self.eat(TokenTypes.DEFAULT)
                is_default = True
                if self.current_token()[0] in ("=", "=="):
                    self.eat(TokenTypes.LOW_PRIORITY_OPERATOR)[1]
                    
                token_type, token_value = self.current_token()
                if token_type:
                    if token_type == TokenTypes.DATE_AND_TIME:
                        if token_value == TokenTypes.CURRENT_DATE:
                            self.eat(TokenTypes.DATE_AND_TIME)
                            defaults[col_name] = schema[col_name](TokenTypes.CURRENT_DATE)
                        elif token_value == TokenTypes.NOW:
                            self.eat(TokenTypes.DATE_AND_TIME)
                            defaults[col_name] = schema[col_name](TokenTypes.NOW)
                        elif token_value == TokenTypes.CURRENT_TIME:
                            self.eat(TokenTypes.DATE_AND_TIME)
                            defaults[col_name] = schema[col_name](TokenTypes.CURRENT_TIME)
                    else:
                        default_value = self.eat(token_type)[1]
                        defaults[col_name] = schema[col_name](default_value)
            else:
                is_default = False
            if self.current_token() and self.current_token()[0] == TokenTypes.CONSTRAINT:
                contr = self.eat(TokenTypes.CONSTRAINT)[1]
                if contr == TokenTypes.NOT_NULL and is_default:
                    raise ValueError("if a Column has DEFAULT VALUE it cannot inheritance  NOT NULL constraints")
                constraints[col_name] = contr
                constr_id = None
                if contr == TokenTypes.PRIMARY_KEY:
                    constr_id = 'pkey'
                elif contr == TokenTypes.NOT_NULL:
                    constr_id = '!null'
                elif contr == TokenTypes.UNIQUE:
                    constr_id = 'ukey'
                key = f"{table_name}_{col_name}_{constr_id}"
                if col_name not in private_constraints:
                    private_constraints[col_name] = set()
                private_constraints[col_name].add(key)
                constraints_ptr[key] = contr
                
            
            if self.current_token() and self.current_token()[0] == TokenTypes.RESTRICTION:
                self.eat(TokenTypes.RESTRICTION)
                expr = self.parse_expression(context=None)
                restrictions[col_name] = expr
                key = f"{table_name}_{col_name}_check"
                if col_name not in private_constraints:
                    private_constraints[col_name] = set()
                private_constraints[col_name].add(key)
                constraints_ptr[key] = TokenTypes.CHECK
                
                
            # Skip comma if present
            if self.current_token() and self.current_token()[0] == TokenTypes.COMMA:
                self.eat(TokenTypes.COMMA)

        self.eat(TokenTypes.CLOSE_PAREN)  # )
        self.eat(TokenTypes.SEMICOLON)
    
        return CreateTableStatement(table_name, schema, defaults, auto, constraints, restrictions, private_constraints, constraints_ptr)
        
    def create_view(self):
        can_be_replaced = False
        self.eat(TokenTypes.CREATE)
        if self.current_token()[1] in Lexer.logical_operators:
            self.eat(self.current_token()[0])
            self.eat(TokenTypes.REPLACE)
            can_be_replaced = True
        if self.current_token()[0] == TokenTypes.MATERIALIZED:
            return self.parse_cmv()
        else:
            self.eat(TokenTypes.VIEW)
            view_name = self.eat(TokenTypes.IDENTIFIER)[1]
            self.eat(TokenTypes.AS)
            expr = self.parse_single_select()
            return CreateView(view_name, query=expr, can_be_replaced=can_be_replaced)
    
    def parse_calling_expression(self):
        self.eat("CALL")
        if self.current_token()[0] == TokenTypes.VIEW:
            self.eat(TokenTypes.VIEW)
            if self.current_token()[0] == TokenTypes.STAR:
                self.eat(TokenTypes.STAR)
                view_name = self.eat(TokenTypes.IDENTIFIER)[1]
            elif self.current_token()[0] == TokenTypes.OPEN_PAREN:
                self.eat(TokenTypes.OPEN_PAREN)
                view_name = self.eat(TokenTypes.IDENTIFIER)[1]
                self.eat(TokenTypes.CLOSE_PAREN)
            else:
                view_name = self.eat(TokenTypes.IDENTIFIER)[1]
                
        return CallView(view_name)
    
    def parse_cta(self, table_name):
        with_data = True
        self.eat(TokenTypes.AS)
        expr = self.parse_single_select()
        if self.current_token() and self.current_token()[0] == TokenTypes.WITH:
            self.eat(TokenTypes.WITH)
            if self.current_token()[0] == TokenTypes.NO:
                self.eat(TokenTypes.NO)
                with_data = False
            self.eat(TokenTypes.DATA)
        return CTA(table_name, query=expr, with_data=with_data)
        
    def parse_cmv(self):
        self.eat(TokenTypes.MATERIALIZED)
        self.eat(TokenTypes.VIEW)
        mt_table_name = self.eat(TokenTypes.IDENTIFIER)[1]
        self.eat(TokenTypes.AS)
        expr = self.parse_single_select()
        return CreateMaterializedView(table_name=mt_table_name, query=expr)
        
    def parse_refresh_mv(self):
        self.eat(TokenTypes.REFRESH)
        self.eat(TokenTypes.MATERIALIZED)
        self.eat(TokenTypes.VIEW)
        mv_name = self.eat(self.current_token()[0])[1]
        return RefreshMaterializedView(mt_view_name=mv_name)
    
    def parse_cte(self):
        cte_expressions = []
        cte_queries = None
        self.eat(TokenTypes.WITH)
        while self.current_token()[0] == TokenTypes.IDENTIFIER:
            cte_name = self.eat(TokenTypes.IDENTIFIER)[1]
            self.eat(TokenTypes.AS)
            self.eat(TokenTypes.OPEN_PAREN)
            cte_query = self.parse_single_select()
            self.eat(TokenTypes.CLOSE_PAREN)
            cte_expressions.append(WithCTExpression(cte_name=cte_name, query=cte_query))
            if self.current_token()[0] == TokenTypes.COMMA:
                self.eat(TokenTypes.COMMA)
            else:
                break
        cte_queries = self.parse_single_select()
        return WithCTE(cte_expressions, cte_queries)
            
        
        
        
        
    def parse_list_viws(self):
        self.eat(TokenTypes.VIEWS)
        return db_manager.list_views()
        

    def parse_drop_database(self):
        self.eat(TokenTypes.DROP)
        self.eat(TokenTypes.DATABASE)
        db_name = self.eat(TokenTypes.IDENTIFIER)[1]
        return DropDatabase(database_name=db_name)
    
    def parse_drop_table(self):
        self.eat(TokenTypes.DROP)
        self.eat(TokenTypes.TABLE)
        table_name = self.eat(TokenTypes.IDENTIFIER)[1]
        return DropTable(table_name=table_name)
    
    def parse_drop_view(self):
        self.eat(TokenTypes.DROP)
        self.eat(TokenTypes.VIEW)
        view_name = self.eat(TokenTypes.IDENTIFIER)[1]
        return DropView(view_name)
    
    def parse_drop_mtv(self):
        self.eat(TokenTypes.DROP)
        self.eat(TokenTypes.MATERIALIZED)
        self.eat(TokenTypes.VIEW)
        view_name = self.eat(TokenTypes.IDENTIFIER)[1]
        return DropMTView(view_name)
    
    def parse_truncate_table(self):
        self.eat(TokenTypes.TRUNCATE)
        self.eat(TokenTypes.TABLE)
        table_name = self.eat(TokenTypes.IDENTIFIER)[1]
        return TruncateTable(table_name)
    
    def parse_use_statement(self):
        self.eat(TokenTypes.USE)
        db_name = self.eat(TokenTypes.IDENTIFIER)[1]
        self.eat(TokenTypes.SEMICOLON)
        return UseStatement(db_name)
    
    def parse_add_column(self):
        default_value = None
        constraint_type = None
        constraint_rule = None
        self.eat(TokenTypes.COLUMN)
        column_name = self.eat(TokenTypes.IDENTIFIER)[1]
        data_type = self.eat(TokenTypes.DATATYPE)[1]
        if self.current_token()[0] == TokenTypes.DEFAULT:
            self.eat(TokenTypes.DEFAULT)
            default_value = self.eat(self.current_token()[0])[1]
        if self.current_token()[0] == TokenTypes.CONSTRAINT:
            constraint_type = self.eat(TokenTypes.CONSTRAINT)[1]
        if self.current_token()[0]== TokenTypes.RESTRICTION:
                constraint_type = self.eat(TokenTypes.RESTRICTION)[1]
                constraint_rule = self.parse_expression()
        return AddColumnFromAlterTable(column_name=column_name, datatype=data_type, default=default_value,
                                        constraint=constraint_type, constraint_rule=constraint_rule)
            
    
    def parse_add_constraint(self):
        constraint_rule = None
        self.eat("CONSTRAINT")
        column_name = None
        constraint_name = self.eat(self.current_token()[0])[1]
        if self.current_token()[0] == TokenTypes.CONSTRAINT:
                constraint_type = self.eat(TokenTypes.CONSTRAINT)[1]
                if self.current_token()[0] == TokenTypes.OPEN_PAREN:
                    self.eat(TokenTypes.OPEN_PAREN)
                    column_name = self.eat(TokenTypes.IDENTIFIER)[1]
                    self.eat(TokenTypes.CLOSE_PAREN)
                else:
                    column_name = self.eat(TokenTypes.IDENTIFIER)[1]
        else:
            constraint_type = self.eat(TokenTypes.RESTRICTION)
            constraint_rule = self.parse_expression()
            self.eat(TokenTypes.ON)
            column_name = self.eat(TokenTypes.IDENTIFIER)[1]

        return AddConstraintFromAlterTable(column_name=column_name, constraint_type=constraint_type,
                                           constraint_name=constraint_name, constraint_rule=constraint_rule)
            
          
            
        
    def parse_expression(self, context = None):
        """Parse mathematical expressions with operator precedence"""
        
        return self.parse_logical_condition(context)
    

    def parse_logical_condition(self, context):
        left = self.parse_condition_engine(context)
        while self.current_token() and self.current_token()[1] in Lexer.logical_operators:
            operator = self.eat(self.current_token()[0])[1]
            right = self.parse_condition_engine(context)
            
            if context == TokenTypes.WHERE:
                self.validate_no_aggregate_in_where(left)
                left = ConditionExpr(left, operator, right, context=TokenTypes.WHERE)
            elif context == TokenTypes.HAVING:
                left = ConditionExpr(left, operator, right, context = TokenTypes.HAVING)
            else:
                left = ConditionExpr(left, operator, right, context = None)
        return left
    
    def parse_condition_engine(self, context):
        expression = self.parse_condition(context)
        
        while self.current_token() and self.current_token()[0] == TokenTypes.LIKE:
            is_not = False
            if self.current_token()[1] == TokenTypes.NOT:
                is_not = True
                self.eat(TokenTypes.LIKE)
            self.eat(TokenTypes.LIKE)
            arg = LiteralExpression(self.eat("STRING")[1])
            return LikeCondition(expression, arg, is_not)
        while self.current_token() and self.current_token()[0] == TokenTypes.NULLCHECK:
            is_null = True
            self.eat(TokenTypes.NULLCHECK)
            if self.current_token() and self.current_token()[0] == TokenTypes.NULLCHECK:
                self.eat(TokenTypes.NULLCHECK)
                is_null = False
            self.eat(TokenTypes.NULL)
            return IsNullCondition(expression, is_null=is_null)
    
        while self.current_token() and self.current_token()[0] == TokenTypes.MEMBERSHIP:
            args = []
            is_nott = False
            self.eat(TokenTypes.MEMBERSHIP)
            if self.current_token()[0] == TokenTypes.MEMBERSHIP:
                self.eat(TokenTypes.MEMBERSHIP)
                is_nott = True
            self.eat(TokenTypes.OPEN_PAREN)
            while True:
                arg = self.parse_factor(context)
                args.append(arg)
                if self.current_token() and self.current_token()[0] == TokenTypes.COMMA:
                    self.eat(TokenTypes.COMMA)
                else:
                    self.eat(TokenTypes.CLOSE_PAREN)
                    break
            return Membership(expression, args, is_not=is_nott)
        while self.current_token() and self.current_token()[0] == TokenTypes.BETWEEN:
                    is_nott = False
                    if self.current_token()[1] == TokenTypes.NOT:
                        self.eat(TokenTypes.BETWEEN)
                        is_nott = True
                    if self.current_token() and self.current_token()[0] == TokenTypes.BETWEEN:
                        self.eat(TokenTypes.BETWEEN)
                    lower = self.parse_factor(context)
                    self.eat(TokenTypes.HIGH_PRIORITY_OPERATOR)
                    upper = self.parse_factor(context)
                    return Between(expression, lower, upper, is_not = is_nott)

                
        return expression
    
    def parse_condition(self, context):
        left = self.parse_addition(context)

        while self.current_token() and self.current_token()[1] in ("=", "!=", ">", "<", ">=", "<="):
            operator = self.eat(self.current_token()[0])[1]
            right = self.parse_addition(context)
            
            
            if context == TokenTypes.WHERE:
                self.validate_no_aggregate_in_where(left)
                left = ConditionExpr(left, operator, right, context=TokenTypes.WHERE)
            elif context == TokenTypes.HAVING:
                left = ConditionExpr(left, operator, right, context = TokenTypes.HAVING)
            else:
                left = ConditionExpr(left, operator, right, context = None)
        return left


    def parse_addition(self, context):
        left = self.parse_multiplication(context)
        
        while self.current_token() and self.current_token()[1] in ('+', '-'):
            
            operator = self.current_token()[1]
            self.eat(TokenTypes.MATH_OPERATOR)  # or whatever token type
            right = self.parse_multiplication(context)
            left = BinaryOperation(left, operator, right)
        
        return left


    def parse_multiplication(self, context):
        
        left = self.parse_factor(context)
        
        while self.current_token() and self.current_token()[1] in ('*', '/'):
            
            operator = self.current_token()[1]
            self.eat(self.current_token()[0])
            
            right = self.parse_factor(context)
            left = BinaryOperation(left, operator, right)
        
        return left
    
    
    def parse_factor(self, context = None):

        
        token = self.current_token()
        
        if token[0] == TokenTypes.OPEN_PAREN:  # (
            self.eat(TokenTypes.OPEN_PAREN)
            expr = self.parse_expression(context)  # Recursively parse inside parentheses
            self.eat(TokenTypes.CLOSE_PAREN)
            return expr
        
        elif token[0] == TokenTypes.IDENTIFIER:
            self.eat(TokenTypes.IDENTIFIER)    
            return ColumnExpression(token[1])
        
        elif token[0] == TokenTypes.MATH_FUNC:
            name = self.eat(TokenTypes.MATH_FUNC)[1]
            round_by = None
            self.eat(TokenTypes.OPEN_PAREN)
            expression = self.parse_expression(context)
            if self.current_token()[0] == TokenTypes.COMMA:
                self.eat(TokenTypes.COMMA)
                round_by = int(self.eat("NUMBER")[1])
            self.eat(TokenTypes.CLOSE_PAREN)
            return MathFunction(name, expression, round_by)
            
        elif token[0] == TokenTypes.STRING_FUNC:
            name = self.eat(TokenTypes.STRING_FUNC)[1]
            start = length = None
            self.eat(TokenTypes.OPEN_PAREN)
            expression = self.parse_expression(context)
            if self.current_token()[0] == TokenTypes.COMMA:
                self.eat(TokenTypes.COMMA)
                start = int(self.eat("NUMBER")[1])
                if self.current_token()[0] == TokenTypes.COMMA:
                    self.eat(TokenTypes.COMMA)
                    length = int(self.eat("NUMBER")[1])
            self.eat(TokenTypes.CLOSE_PAREN)
            return StringFunction(name, expression, start, length)
        
        elif token[0] == TokenTypes.REPLACE:
            self.eat(TokenTypes.REPLACE)
            self.eat(TokenTypes.OPEN_PAREN)
            expression = self.parse_expression(context)
            self.eat(TokenTypes.COMMA)
            old = self.parse_expression(context)
            self.eat(TokenTypes.COMMA)
            new = self.parse_expression(context)
            self.eat(TokenTypes.CLOSE_PAREN)
            return Replace(expression, old, new)
        
        elif token[0] == TokenTypes.CONCAT:
            self.eat(TokenTypes.CONCAT)
            expressions = []
            self.eat(TokenTypes.OPEN_PAREN)
            while self.current_token()[0] != TokenTypes.CLOSE_PAREN:
                arg = self.parse_expression()
                expressions.append(arg)
                
                if self.current_token()[0] == TokenTypes.COMMA:
                    self.eat(TokenTypes.COMMA)
            self.eat(TokenTypes.CLOSE_PAREN)
            return Concat(expressions)
        
            
        elif token[0] == TokenTypes.DATE_AND_TIME and token[1] == TokenTypes.DATEDIFF:
            unit = 'days'    
            self.eat(TokenTypes.DATE_AND_TIME)
            self.eat(TokenTypes.OPEN_PAREN)
            date1 = self.parse_expression()
            self.eat(TokenTypes.COMMA)
            date2 = self.parse_expression()
            if self.current_token()[0] == TokenTypes.COMMA:
                self.eat(TokenTypes.COMMA)
                unit = self.parse_expression()
                if not isinstance(unit, LiteralExpression):
                    raise ValueError("unit must be represented as string")
            self.eat(TokenTypes.CLOSE_PAREN)
            return DateDIFF(date1, date2, unit)
        
        
        elif token[0] == TokenTypes.EXTRACT:
            self.eat(TokenTypes.EXTRACT)
            self.eat(TokenTypes.OPEN_PAREN)
            part = self.eat(TokenTypes.DATE_AND_TIME)[1].upper()
            self.eat(TokenTypes.FROM)
            expression = self.parse_expression()
            self.eat(TokenTypes.CLOSE_PAREN)
            return Extract(expression, part)
            

        elif token[0] == TokenTypes.COALESCE:
            expressions = []
            self.eat(TokenTypes.COALESCE)
            self.eat(TokenTypes.OPEN_PAREN)
            while self.current_token() and self.current_token()[0] != TokenTypes.CLOSE_PAREN:
                exp = self.parse_expression()
                expressions.append(exp)
                
                if self.current_token()[0] == TokenTypes.COMMA:
                    self.eat(TokenTypes.COMMA)
                
            self.eat(TokenTypes.CLOSE_PAREN)
            return CoalesceFunction(expressions)
        
        elif token[0] == TokenTypes.NULLIF:
            self.eat(TokenTypes.NULLIF)
            self.eat(TokenTypes.OPEN_PAREN)
            expr = self.parse_expression()
            self.eat(TokenTypes.COMMA)
            number = self.parse_expression()
            self.eat(TokenTypes.CLOSE_PAREN)
            return NullIF(expr, number)
                
        elif token[0] == TokenTypes.SELECT:
            return self.parse_single_select()
        
        elif token[0] == TokenTypes.CAST:
            self.eat(TokenTypes.CAST)
            self.eat(TokenTypes.OPEN_PAREN)
            expression = self.parse_expression()
            if self.current_token()[0] in (TokenTypes.COMMA, TokenTypes.AS):
                self.eat(self.current_token()[0])
            target = self.eat(self.current_token()[0])[1].upper()
            if target not in Lexer.datatypes or target == TokenTypes.SERIAL:
                raise ValueError("Invalid Given Data Type")
            self.eat(TokenTypes.CLOSE_PAREN)
            return Cast(expression, target)
            
        elif token[0] == TokenTypes.DATE_AND_TIME and token[1] == TokenTypes.CURRENT_DATE:
            self.eat(TokenTypes.DATE_AND_TIME)
            return CurrentDate()
        
        
        elif token[0] == TokenTypes.ADD:
            
            self.eat(TokenTypes.ADD)
            if self.current_token()[0] == TokenTypes.COLUMN:
                return self.parse_add_column()
            elif self.current_token()[0] == "CONSTRAINT":
                return self.parse_add_constraint()
        
        elif token[0] == TokenTypes.CASE_WHEN:
            self.eat(TokenTypes.CASE_WHEN)
            expressions = []
            actions = []
            case_else = None
            while self.current_token() and self.current_token()[1] != "END":
                stm = self.eat(TokenTypes.CASE_WHEN)[1] 
                expr = self.parse_expression()
                if stm == "ELSE":
                    case_else = expr
                else:
                    expressions.append(expr)
                    self.eat(TokenTypes.CASE_WHEN)
                    action = self.parse_expression()
                    actions.append(action)
            self.eat(TokenTypes.CASE_WHEN)
            return CaseWhen(expressions, actions, case_else=case_else)            
                    
        
        elif token[0] == TokenTypes.REFERENCE:
            table_alias = self.eat(TokenTypes.REFERENCE)[1]
            self._requested_pointer = table_alias
            self.eat(TokenTypes.DOT)
            table_name = self.eat(self.current_token()[0])[1]
            actual_table = self._pointers.get(table_alias, table_alias)
            return QualifiedColumnExpression(actual_table, table_name)
                
                
        elif token[0] == TokenTypes.EXISTS:
            self.eat(TokenTypes.EXISTS)
            if context != TokenTypes.WHERE:
                raise ValueError("EXISTS Function Works Only With WHERE Clause")
            subquery = self.parse_expression(TokenTypes.WHERE)
            if not isinstance(subquery, SelectStatement):
                raise ValueError('EXISTS function works only with subqueries')
            return Exists(subquery)
                
        
        elif token[0] == TokenTypes.FUNC:
            distinct = False
            name = self.eat(TokenTypes.FUNC)[1]
            self.eat(TokenTypes.OPEN_PAREN)
            if self.current_token() and self.current_token()[0] == TokenTypes.DISTINCT:
                self.eat(TokenTypes.DISTINCT)
                distinct = True
            expression = self.parse_expression(context)
            self.eat(TokenTypes.CLOSE_PAREN)
            return Function(name, expression, distinct=distinct)
        
        elif token[0] in ON_INSERT_UPDATE_ACCEPTED_DATA_TYPES:
            self.eat(self.current_token()[0])
            return LiteralExpression(token[1])
        
        elif token[0] == TokenTypes.STAR:
            self.eat(TokenTypes.STAR)
            return ColumnExpression(token[1])
        
        elif token[0] == TokenTypes.NOT:
            self.eat(TokenTypes.NOT)
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
            # Check if any of the date expressions contain aggregates
            return (self._contains_aggregates(self.date1) if hasattr(self, 'date1') else 
                    (self._contains_aggregates(expr.date1) if hasattr(expr, 'date1') else False) or
                    self._contains_aggregates(expr.date2) if hasattr(expr, 'date2') else False)
        elif isinstance(expr, CaseWhen):
            # Check all WHEN conditions and THEN actions
            for expr_r in expr.expressions:
                if self._contains_aggregates(expr_r):
                    return True
            for expr_r in expr.actions:
                if self._contains_aggregates(expr_r):
                    return True
            if expr.case_else and self._contains_aggregates(expr.case_else):
                return True
            return False
        elif isinstance(expr, Extract):
            return self._contains_aggregates(expr.expression)  
        elif isinstance(expr, BinaryOperation):
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