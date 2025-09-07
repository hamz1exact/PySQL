from datetime import datetime, time
# from errors import 
# class SQLType:
#     def __init__(self, value=None):
#         if value is not None:
#             self.value = self.parse(value)
#             self.validate(self.value)
#             self.sqltype = self.Sqltype()
            
#         else:
#             self.value = self.parse(value)
#             self.sqltype = self.Sqltype()
class SQLType:
    def __init__(self, value=None):
        if value is not None:
            self.value = self.parse(value)
            self.validate(self.value)
            self.sqltype = self.Sqltype()
        else:
            self.value = None
            # self.sqltype = self.Sqltype()
    def __eq__(self, other):
        return self.value == (other.value if isinstance(other, SQLType) else other)

    def __ne__(self, other):
        return self.value != (other.value if isinstance(other, SQLType) else other)

    def __lt__(self, other):
        return self.value < (other.value if isinstance(other, SQLType) else other)

    def __le__(self, other):
        return self.value <= (other.value if isinstance(other, SQLType) else other)

    def __gt__(self, other):
        return self.value > (other.value if isinstance(other, SQLType) else other)

    def __ge__(self, other):
        return self.value >= (other.value if isinstance(other, SQLType) else other)

    # ---------------- Arithmetic (optional) ----------------
    def __add__(self, other):
        return self.__class__(self.value + (other.value if isinstance(other, SQLType) else other))

    def __sub__(self, other):
        return self.__class__(self.value - (other.value if isinstance(other, SQLType) else other))

    def __mul__(self, other):
        return self.__class__(self.value * (other.value if isinstance(other, SQLType) else other))

    def __truediv__(self, other):
        return self.__class__(self.value / (other.value if isinstance(other, SQLType) else other))

        
class INT(SQLType):
    def parse(self, value):
        if value == "NULL":
            return str(value).upper()
        if value == None:
            return None
        if isinstance(value, int):
            return value
        
        if isinstance(value, float):
            if value.is_integer():
                return int(value)
            raise ValueError(f"Cannot convert non-integer float '{value}' to INT")
        
        if isinstance(value, str):
            if "." in value:
                f = float(value)
                if f.is_integer():
                    return int(f)
                raise ValueError(f"Cannot convert non-integer string '{value}' to INT")
            return int(value)
        
        raise ValueError(f"Cannot convert {value} of type {type(value)} to INT")
    
    def validate(self, value):
        if isinstance(value, str):
            pass
        elif not isinstance(value, int) or isinstance(value, bool):
            raise ValueError(f"INT expects an integer, got {value} ({type(value)})")
    
    def Sqltype(self):
        return "class '<int>'"
        

    
class FLOAT(SQLType):
    def parse(self, value):
        
        # if already float or int, just convert to float
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            return float(value)
        
        # if string, try to parse as float
        if isinstance(value, str):
            try:
                return float(value)
            except ValueError:
                raise ValueError(f"Cannot convert '{value}' to FLOAT")
        
        # anything else → reject
        raise ValueError(f"Cannot convert {value} of type {type(value)} to FLOAT")
    
    def validate(self, value):
        if not isinstance(value, float) or isinstance(value, bool):
                raise ValueError(f"FLOAT expects an float, got {value} ({type(value)})")
    def Sqltype(self):
        return "class '<float>'"

class BOOLEAN(SQLType):
    def parse(self, value):
        # Already a bool → accept
        if isinstance(value, bool):
            return value
        
        # Numeric 0 or 1 → convert
        if isinstance(value, int):
            if value == 0:
                return False
            elif value == 1:
                return True
            else:
                raise ValueError(f"Cannot convert integer '{value}' to BOOLEAN")
        
        # String values → common SQL mappings
        if isinstance(value, str):
            v = value.strip().lower()
            if v in ("true", "t", "1"):
                return True
            elif v in ("false", "f", "0"):
                return False
            else:
                raise ValueError(f"Cannot convert string '{value}' to BOOLEAN")
        
        # Anything else → reject
        raise ValueError(f"Cannot convert {value} of type {type(value)} to BOOLEAN")
    
    def validate(self, value):
        if not isinstance(value, bool):
            raise ValueError(f"BOOLEAN expects a boolean, got {value} ({type(value)})")
    def Sqltype(self):
        return "class '<bool>'"
        
class CHAR(SQLType):
    def parse(self, value):
        # Convert everything to string first
        if not isinstance(value, str):
            value = str(value)
        return value
    def validate(self, value):
        if not isinstance(value, str) or len(value) != 1:
            raise ValueError(f"CHAR expects a single character, got '{value}' (length {len(value)})")
    def Sqltype(self):
        return "<class 'char'>"

class VARCHAR(SQLType):
    def parse(self, value):
        if not isinstance(value, str):
            value = str(value)
        return value
    def validate(self, value):
        if not isinstance(value, str):
            raise ValueError(f"VARCHAR expects a string, got {value} ({type(value)})")
    def Sqltype(self):
        return type(self.value)
        
class TEXT(SQLType):
    def parse(self, value):
        # Convert anything to string
        if not isinstance(value, str):
            value = str(value)
        return value

    def validate(self, value):
        if not isinstance(value, str):
            raise ValueError(f"TEXT expects a string, got {value} ({type(value)})")
    def Sqltype(self, value):
        return type(value)

from datetime import datetime, date

class DATE(SQLType):
    def parse(self, value):
        # If the value is already a date, just return it
        if isinstance(value, date):
            return value
        # If it’s a string, try to parse
        if isinstance(value, str):
            try:
                return datetime.strptime(value, "%Y-%m-%d").date()
            except ValueError:
                raise ValueError(f"Invalid DATE format: {value}. Expected YYYY-MM-DD.")
        # Any other type is invalid
        raise ValueError(f"Cannot convert {value} ({type(value)}) to DATE.")

    def validate(self, value):
        # Validation just calls parse (raises error if invalid)
        self.parse(value)
    def Sqltype(self):
        return "<class 'date'>"
            

class TIME(SQLType):
    def parse(self, value):
        # If already a time object → keep it
        if isinstance(value, time):
            return value
        
        # If string → try parsing
        if isinstance(value, str):
            try:
                return datetime.strptime(value, "%H:%M:%S").time()
            except ValueError:
                raise ValueError(f"Cannot convert '{value}' to TIME. Format must be HH:MM:SS")
        
        # Anything else → reject
        raise ValueError(f"Cannot convert {value} of type {type(value)} to TIME")
    
    def validate(self, value):
        if not isinstance(value, time):
            raise ValueError(f"TIME expects a datetime.time object, got {value} ({type(value)})")
    def Sqltype(self):
        return "<class 'time'>"
        
class SERIAL(SQLType):
    def __init__(self, value=None):
        super().__init__(value)
        self.current = 1 if value is None else int(value)

    def parse(self, value):
        # ignore user value if None
        if value is None:
            return self.next()
        return int(value)

    def next(self):
        val = self.current
        self.current += 1
        return val

    def __str__(self):
        return str(self.current)

    def validate(self, value):
        if not isinstance(value, int) or isinstance(value, bool):
            raise ValueError(f"SERIAL expects an integer, got {value} ({type(value)})")
        
    def Sqltype(self):
        return "<class 'auto_increment'>"
        
class NULLVALUE(SQLType):
    def __init__(self, value=None):
        super().__init__(value)
    
    def parse(self, value):
        if value is None or value.lower() in ("none", 'null'):
            value = str("NULL")
            return value
        else:
            raise ValueError('error from NONE class Type')
        
    def validate(self, value):
        pass
    
    def Sqltype(self):
        return type(None)
        
    

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
    "NONE": NULLVALUE
}

x = 0
map = {x: SERIAL(10)}
print(map[x].next())
