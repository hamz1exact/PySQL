import os
import platform
import msgpack
from datatypes import datatypes, SERIAL # assuming Lexer.datatypes contains your SQLType classes
import random
class Table:
    def __init__(self, name, schema, defaults=None, auto=None, constraints = None, restrictions = None):
        self.name = name
        self.schema = schema                  # dict[col_name] = SQLType class
        self.defaults = defaults or {}        # dict[col_name] = SQLType instance
        self.auto = auto or {}                # dict[col_name] = SQLType instance (SERIAL)
        self.rows = []                       # list of dicts with parsed Python values
        self.constraints = constraints or {}
        self.restrictions = restrictions or {}

class DatabaseManager:
    def __init__(self):
        home = os.path.expanduser("~")

        if platform.system() == "Windows":
            self.db_folder = os.path.join(os.getenv("APPDATA"), "su_sql")
        else:
            self.db_folder = os.path.join(home, ".su_sql")

        # Hidden cache file
        self.cache_file = os.path.join(self.db_folder, ".su_cache")
        os.makedirs(self.db_folder, exist_ok=True)

        self.databases = []
        self.active_db_name = None
        self.active_db = {}

        self.load_cache()
        self.auto_use_recent_db()

    # ---------------- Cache Handling ----------------
    def load_cache(self):
        if os.path.exists(self.cache_file):
            with open(self.cache_file, "rb") as f:
                cache = msgpack.unpack(f)
                self.databases = cache.get("databases", [])
                self.recent = cache.get("recent", None)
        else:
            self.databases = []
            self.recent = None

    def update_cache(self):
        cache = {
            "databases": self.databases,
            "recent": self.active_db_name
        }
        with open(self.cache_file, "wb") as f:
            msgpack.pack(cache, f)

    def auto_use_recent_db(self):
        if hasattr(self, "recent") and self.recent and os.path.exists(self.recent):
            self.active_db_name = self.recent
            self.load_database_file()

    # ---------------- Database Operations ----------------
    def create_database(self, db_name):
        db_file = os.path.join(self.db_folder, f"{db_name}.su")
        if os.path.exists(db_file):
            raise ValueError(f"Database '{db_name}' already exists")
        self.databases.append(db_file)
        self.active_db_name = db_file
        with open(db_file, "wb") as f:
            msgpack.pack({}, f)
        self.update_cache()
        print(f"Database '{db_name}' created and selected")

    def use_database(self, db_name):
        db_file = os.path.join(self.db_folder, f"{db_name}.su")
        if db_file not in self.databases:
            raise ValueError(f"Database '{db_name}' does not exist")
        self.active_db_name = db_file
        self.load_database_file()
        self.update_cache()
        print(f"Now using database '{db_name}'")

    # ---------------- Table I/O ----------------
    def load_database_file(self):
        try:
            with open(self.active_db_name, "rb") as f:
                db_data = msgpack.unpack(f)

            self.active_db = {}
            for tbl_name, tbl_data in db_data.items():
                # Reconstruct schema classes (same as before)
                schema = {col: datatypes[tbl_data["schema"][col]] for col in tbl_data["schema"]}
                
                # Reconstruct defaults using deserialize_value for complex objects
                defaults = {}
                for col in tbl_data.get("defaults", {}):
                    raw_value = tbl_data["defaults"][col]
                    deserialized_value = deserialize_value(raw_value)
                    
                    # If it's a simple value, wrap it with SQLType
                    if not hasattr(deserialized_value, '__dict__') or isinstance(deserialized_value, (str, int, float, bool)):
                        defaults[col] = schema[col](deserialized_value)
                    else:
                        # It's already a complex object (like ConditionExpr), keep as is
                        defaults[col] = deserialized_value

                # Reconstruct auto values
                auto = {}
                for col in tbl_data.get("auto", {}):
                    raw_value = tbl_data["auto"][col]
                    deserialized_value = deserialize_value(raw_value)
                    
                    if not hasattr(deserialized_value, '__dict__') or isinstance(deserialized_value, (str, int, float, bool)):
                        auto[col] = schema[col](deserialized_value)
                    else:
                        auto[col] = deserialized_value
                
                # Reconstruct constraints - these are likely ConditionExpr objects
                constraints = {}
                for col in tbl_data.get("constraints", {}):
                    raw_value = tbl_data["constraints"][col]
                    deserialized_value = deserialize_value(raw_value)
                    # ConditionExpr objects should be kept as-is after deserialization
                    constraints[col] = deserialized_value
                
                # Reconstruct restrictions - these are also likely ConditionExpr objects  
                restrictions = {}
                for col in tbl_data.get("restrictions", {}):
                    raw_value = tbl_data["restrictions"][col]
                    deserialized_value = deserialize_value(raw_value)
                    restrictions[col] = deserialized_value

                # Reconstruct rows
                rows = []
                for row_dict in tbl_data.get("rows", []):
                    row = {}
                    for col in row_dict:
                        raw_value = row_dict[col]
                        deserialized_value = deserialize_value(raw_value)
                        
                        # For row data, we usually want SQLType objects
                        if not hasattr(deserialized_value, '__dict__') or isinstance(deserialized_value, (str, int, float, bool)):
                            row[col] = schema[col](deserialized_value)
                        else:
                            # Complex object - keep as is
                            row[col] = deserialized_value
                    rows.append(row)

                table = Table(tbl_name, schema, defaults, auto, constraints, restrictions)
                table.rows = rows

                # ✅ Fix SERIAL counters
                for col, col_type in schema.items():
                    if col_type == SERIAL:  # check if this column is SERIAL
                        max_val = 0
                        for row in rows:
                            if row[col] is not None:
                                max_val = max(max_val, int(row[col].value))
                        if col in table.auto:
                            table.auto[col].current = max_val + 1  # resume from last

                self.active_db[tbl_name] = table
                
        except Exception as e:
            print(f'Your Database is damaged ({e}), we create a new one for you, you can DROP IT anytime')
            self.create_database(f"test_{random.randint(1,1000000)}")

    def save_database_file(self):  
        try:
            db_data = {}
            
            for tbl_name, table in self.active_db.items():
                db_data[tbl_name] = {
                    # Save class names for schema
                    "schema": {col: table.schema[col].__name__ for col in table.schema},
                    # Serialize all complex objects using our enhanced function
                    "defaults": {col: serialize_value(table.defaults[col]) for col in table.defaults},
                    "auto": {col: serialize_value(table.auto[col]) for col in table.auto},
                    "constraints": {col: serialize_value(table.constraints[col]) for col in table.constraints},
                    "restrictions": {col: serialize_value(table.restrictions[col]) for col in table.restrictions},
                    "rows": [
                        {col: serialize_value(row[col]) for col in row}
                        for row in table.rows
                    ]
                }
            
            with open(self.active_db_name, 'wb') as f:
                msgpack.pack(db_data, f)
                
            
        except Exception as e:
            print(f"Error saving database: {e}")
            raise
            
            

def serialize_value(value):
    """Generic serialization that handles any object structure"""
    if value is None:
        return None
    elif isinstance(value, (str, int, float, bool, list)):
        return value
    elif isinstance(value, dict):
        # Recursively serialize dictionary values
        return serialize_dict(value)
    elif isinstance(value, (datetime, date, time)):
        return {
            '__type__': 'datetime_obj',
            '__class__': value.__class__.__name__,
            '__value__': value.isoformat()
        }
    elif hasattr(value, '__dict__'):
        # Handle any custom object with attributes (like ConditionExpr)
        return {
            '__type__': 'custom_object',
            '__class__': value.__class__.__name__,
            '__module__': value.__class__.__module__,
            '__data__': serialize_dict(value.__dict__)
        }
    else:
        # Fallback: convert to string
        return {
            '__type__': 'string_fallback',
            '__value__': str(value)
        }

def serialize_dict(data):
    """Recursively serialize dictionary values"""
    result = {}
    for key, value in data.items():
        result[key] = serialize_value(value)
    return result
from datetime import datetime, date, time

def deserialize_value(value):
    """Generic deserialization"""
    if not isinstance(value, dict):
        return value
    
    if '__type__' not in value:
        # Regular dictionary - deserialize its values
        result = {}
        for key, val in value.items():
            result[key] = deserialize_value(val)
        return result
    
    value_type = value['__type__']
    
    if value_type == 'datetime_obj':
        # Recreate datetime objects
        class_name = value['__class__']
        iso_value = value['__value__']
        
        if class_name == 'datetime':
            return datetime.fromisoformat(iso_value)
        elif class_name == 'date':
            return datetime.fromisoformat(iso_value).date()
        elif class_name == 'time':
            return datetime.fromisoformat(f"1970-01-01T{iso_value}").time()
    
    elif value_type == 'custom_object':
        # Recreate custom objects like ConditionExpr
        class_name = value['__class__']
        module_name = value.get('__module__')
        data = value['__data__']
        
        # Deserialize nested data first
        deserialized_data = {}
        for key, val in data.items():
            deserialized_data[key] = deserialize_value(val)
        
        # Try to recreate the object
        try:
            if module_name and module_name != '__main__':
                module = __import__(module_name, fromlist=[class_name])
                cls = getattr(module, class_name)
            else:
                # Try to find the class in globals (for classes defined in main script)
                import sys
                cls = None
                for module in sys.modules.values():
                    if hasattr(module, class_name):
                        cls = getattr(module, class_name)
                        break
                
                if not cls:
                    # Last resort: try to import from common places
                    try:
                        from sql_ast import ConditionExpr  # adjust this import as needed
                        if class_name == 'ConditionExpr':
                            cls = ConditionExpr
                    except ImportError:
                        pass
            
            if cls:
                # Create instance and set attributes
                obj = cls.__new__(cls)
                for key, val in deserialized_data.items():
                    setattr(obj, key, val)
                return obj
            else:
                print(f"Warning: Could not find class {class_name}, returning raw data")
                return deserialized_data
                
        except Exception as e:
            print(f"Warning: Could not recreate {class_name}: {e}")
            # Return the raw data as fallback
            return deserialized_data
    
    elif value_type == 'string_fallback':
        return value['__value__']
    
    # Default: return as-is
    return value