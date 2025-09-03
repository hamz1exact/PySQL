from datetime import datetime

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
    
    

def CheckDataType(col_type):
    checker = {str:("PLAINSTR", "TEXT", "STR", "CHAR", "TIME", "DATE"),
        int: ("INT", "FLOAT", "AUTO_INT"),
        bool: ("BOOLEAN")}
    for key in checker:
        if col_type in checker[key]:
            return key