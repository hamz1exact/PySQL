from datetime import datetime
import re
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
        int: ("INT", "AUTO_INT"),
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
    #     int: ("INT", "AUTO_INT"),
    #     bool: ("BOOLEAN")
    #          }
    py_type = CheckDataType(col_type)
    if py_type in (int, float) and isinstance(value, (int, float)):
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
    
print(DataType_evaluation("FLOAT", 1))

        
    
    
