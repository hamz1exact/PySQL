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
    if len(inp) != 1 or not isinstance(inp, str):
        return False
    return True

def PlainstringChecker(inp):
    if not re.fullmatch(r"[A-Za-z ]+", inp) or not not isinstance(inp, str):
        return False
    return True

def CheckDataType(col_type):
    checker = {str:("PLAINSTR", "TEXT", "STR", "CHAR", "TIME", "DATE"),
        int: ("INT", "AUTO_INT", "FLOAT"),
        float:("FLOAT", "INT"),
        bool: ("BOOLEAN")}
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
    if CheckDataType(col_type) == type(value):
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

        
    
    
