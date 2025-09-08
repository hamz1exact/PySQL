from datetime import datetime
import re
    
# """    checker = {
#         str: ("PLAINSTR", "TEXT", "STR", "CHAR", "TIME", "DATE"),
#         int: ("INT", "SERIAL"),
#         float: ("FLOAT", "DECIMAL", "DOUBLE"),
#         bool: ("BOOLEAN")
#     } """

def getSchemaDataType(schema_val):
    if schema_val in ("INT", "SERIAL"):
        return int
    if schema_val in ("FLOAT", "DECIMAL", "DOUBLE"):
        return float
    if schema_val == "BOOLEAN":
        return bool
    if schema_val in ("PLAINSTR", "TEXT", "STR", "CHAR", "TIME", "DATE"):
        return str
    
    
def CheckDate(inp):
        try:
            if datetime.strptime(inp, "%Y-%m-%d").date():
                return True
        except Exception:
            return False
def CheckTime(inp):
    try:
        if datetime.strptime(inp, "%H:%M:%S").time():
            return True
    except Exception:
        return False
    
    
def CharChecker(inp):
    if not isinstance(inp, str) or len(inp) != 1:
        return False
    return True

def PlainstringChecker(inp):
    if not isinstance(inp, str) or not re.fullmatch(r"[A-Za-z ]+", inp):
        return False
    return True

def CheckDataType(col_type):
    checker = {
        str: ("PLAINSTR", "TEXT", "STR", "CHAR", "TIME", "DATE"),
        int: ("INT"),
        float: ("FLOAT", "DECIMAL", "DOUBLE"),
        bool: ("BOOLEAN")
    }
    col_type = str(col_type).upper()
    for key in checker:
        if col_type in checker[key]:
            return key
    return None


def DataType_evaluation(col_type, value):
    # checker = {
    #     str:("PLAINSTR", "TEXT", "STR", "CHAR", "TIME", "DATE"),
    #     int: ("INT", "SERIAL"),
    #     bool: ("BOOLEAN")
    #          }
    py_type = CheckDataType(col_type)
    if py_type in (int, float) and type(value) in (int, float):
        return True
    elif CheckDataType(col_type) == type(value):
        if col_type == "CHAR":
            return CharChecker(value)
        if col_type == "TIME":
            return CheckTime(value)
        if col_type == "DATE":
            return CheckDate(value)
        if col_type == "PLAINSTR":
            return PlainstringChecker(value)
        else:
            return True
    else:
        return False
    
def data_validator(schema_col, schema_val, given_val):
    curr_val = schema_val.upper()
    if curr_val == "CHAR":
        if not CharChecker(given_val):
            if type(given_val) == str:
                raise ValueError(f"{schema_col} Column has <class 'char'> Datatype, a <class 'char'> length must be exactly 1 character, but {len(given_val)} chars were given")
            else:
                raise ValueError(f"{schema_col} Expect <class 'char'> DataType, But {type(given_val)} were given")
        else:
            return True
    elif curr_val == "PLAINSTR":
        if not PlainstringChecker(given_val):
            if isinstance(given_val, str):
                raise ValueError(f"Invalud Values '{given_val}' for columns {schema_col}, input must be  a string")
            else:
                raise ValueError(f"{schema_col} expect PlainString Input, so the input must be contain only leters and spaces")
    elif curr_val == "TIME":
        if not CheckTime(given_val):
            raise ValueError (f"Invalid Value -> {given_val} for columns -> {schema_col}: input must be in format HH:MM:SS")
        else:
            return True
    elif curr_val == "DATE":
        if not CheckDate(given_val):
            raise ValueError(f"Invalid Value -> {given_val} for columns -> {schema_col}: input must be in format -> YYYY:MM:DD")
    elif not DataType_evaluation(schema_val, given_val):
        raise ValueError(f"Column -> '{schema_col}' Expect 'class <{schema_val.lower()}>' DataType, But {type(given_val)} were given")
    return True
