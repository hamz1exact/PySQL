import os
import platform
import msgpack
from datatypes import datatypes, SERIAL # assuming Lexer.datatypes contains your SQLType classes
import random
class Table:
    def __init__(self, name, schema, defaults=None, auto=None, constraints = None, restrictions = None, private_constraints = None, constraints_ptr = None):
        self.name = name
        self.schema = schema                  # dict[col_name] = SQLType class
        self.defaults = defaults or {}        # dict[col_name] = SQLType instance
        self.auto = auto or {}                # dict[col_name] = SQLType instance (SERIAL)
        self.rows = []                       # list of dicts with parsed Python values
        self.constraints = constraints or {}
        self.restrictions = restrictions or {}
        self.private_constraints = private_constraints or {}
        self.constraints_ptr = constraints_ptr or {}
        

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
        self.views = {}

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

    # ---------------- Table I/O ----------------
    def save_database_file(self):
        try:
            
            
            # Create the database structure
            db_data = {}
            
            # Serialize regular tables
            for tbl_name, table in self.active_db.items():
                
                db_data[tbl_name] = {
                    "schema": {col: table.schema[col].__name__ for col in table.schema},
                    "defaults": {col: deep_serialize(table.defaults[col]) for col in table.defaults},
                    "auto": {col: deep_serialize(table.auto[col]) for col in table.auto},
                    "constraints": {col: deep_serialize(table.constraints[col]) for col in table.constraints},
                    "restrictions": {col: deep_serialize(table.restrictions[col]) for col in table.restrictions},
                    "private_constraints": {col: deep_serialize(table.private_constraints[col]) for col in table.private_constraints},
                    "constraints_ptr": {col: deep_serialize(table.constraints_ptr[col]) for col in table.constraints_ptr},
                    "rows": [
                        {col: deep_serialize(row[col]) for col in row}
                        for row in table.rows
                    ]
                }
            
            # Serialize views
            if self.views:
            
                db_data["__views__"] = {}
                for view_name, view_obj in self.views.items():
            
                    db_data["__views__"][view_name] = deep_serialize(view_obj)
            
            
            
            
            # Save to file
            with open(self.active_db_name, 'wb') as f:
                msgpack.pack(db_data, f)
            
            
            
        except Exception as e:
            print(f"Error saving database: {e}")
            import traceback
            traceback.print_exc()
            raise

    def load_database_file(self):
        try:
            with open(self.active_db_name, "rb") as f:
                db_data = msgpack.unpack(f)
            
            
            
            self.views = {}
            self.active_db = {}
            
            for tbl_name, tbl_data in db_data.items():
                if tbl_name == "__views__":
                
                    for view_name, serialized_ast in tbl_data.items():
                        try:
                            
                            deserialized_view = deep_deserialize(serialized_ast)
                            self.views[view_name] = deserialized_view
                
                        except Exception as e:
                            print(f"Warning: Could not deserialize view '{view_name}': {e}")
                    continue
                
                
                # Handle regular tables (your existing logic, but with deep_deserialize)
                schema = {col: datatypes[tbl_data["schema"][col]] for col in tbl_data["schema"]}
                
                defaults = {}
                for col in tbl_data.get("defaults", {}):
                    raw_value = tbl_data["defaults"][col]
                    deserialized_value = deep_deserialize(raw_value)
                    if not hasattr(deserialized_value, '__dict__') or isinstance(deserialized_value, (str, int, float, bool)):
                        defaults[col] = schema[col](deserialized_value)
                    else:
                        defaults[col] = deserialized_value

                # Continue with your existing logic but use deep_deserialize for everything...
                auto = {}
                for col in tbl_data.get("auto", {}):
                    raw_value = tbl_data["auto"][col]
                    deserialized_value = deep_deserialize(raw_value)
                    if not hasattr(deserialized_value, '__dict__') or isinstance(deserialized_value, (str, int, float, bool)):
                        auto[col] = schema[col](deserialized_value)
                    else:
                        auto[col] = deserialized_value
                
                constraints = {}
                for col in tbl_data.get("constraints", {}):
                    raw_value = tbl_data["constraints"][col]
                    deserialized_value = deep_deserialize(raw_value)
                    constraints[col] = deserialized_value
                
                restrictions = {}
                for col in tbl_data.get("restrictions", {}):
                    raw_value = tbl_data["restrictions"][col]
                    deserialized_value = deep_deserialize(raw_value)
                    restrictions[col] = deserialized_value
                    
                private_constraints = {}
                for col in tbl_data.get("private_constraints", {}):
                    raw_value = tbl_data["private_constraints"][col]
                    deserialized_value = deep_deserialize(raw_value)
                    private_constraints[col] = deserialized_value
                
                constraints_ptr = {}
                for col in tbl_data.get("constraints_ptr", {}):
                    raw_value = tbl_data["constraints_ptr"][col]
                    deserialized_value = deep_deserialize(raw_value)
                    constraints_ptr[col] = deserialized_value

                rows = []
                for row_dict in tbl_data.get("rows", []):
                    row = {}
                    for col in row_dict:
                        raw_value = row_dict[col]
                        deserialized_value = deep_deserialize(raw_value)
                        if not hasattr(deserialized_value, '__dict__') or isinstance(deserialized_value, (str, int, float, bool)):
                            row[col] = schema[col](deserialized_value)
                        else:
                            row[col] = deserialized_value
                    rows.append(row)

                table = Table(tbl_name, schema, defaults, auto, constraints, restrictions, private_constraints, constraints_ptr)
                table.rows = rows

                # Fix SERIAL counters
                for col, col_type in schema.items():
                    if col_type == SERIAL:
                        max_val = 0
                        for row in rows:
                            if row[col] is not None:
                                max_val = max(max_val, int(row[col].value))
                        if col in table.auto:
                            table.auto[col].current = max_val + 1

                self.active_db[tbl_name] = table
            
            
                    
        except Exception as e:
            print(f'Database loading failed ({e}), creating new database')
            import random
            self.create_database(f"test_{random.randint(1,1000000)}")
        

    # Also replace the old serialize_value and deserialize_value functions
    
            
            
    def create_view(self, view_name, select_ast):
        if view_name in self.views:
            raise ValueError(f"View '{view_name}' already exists")
        self.views[view_name] = select_ast
        self.save_database_file()
        print(f"View '{view_name}' created.")

    def drop_view(self, view_name):
        if view_name not in self.views:
            raise ValueError(f"View '{view_name}' does not exist")
        del self.views[view_name]
        self.save_database_file()
        print(f"View '{view_name}' dropped.")

    def list_views(self):
        if not self.views:
            print("No views defined.")
        else:
            print("Available Views:")
            for v in self.views:
                print(f"• {v}")
                


from datetime import datetime, date, time
def deep_serialize(obj):
    """
    Recursively convert ALL objects to msgpack-compatible types.
    This ensures no custom objects slip through.
    """
    if obj is None:
        return None
    elif isinstance(obj, (str, int, float, bool)):
        return obj
    elif isinstance(obj, (list, tuple)):
        return [deep_serialize(item) for item in obj]
    elif isinstance(obj, dict):
        return {str(key): deep_serialize(value) for key, value in obj.items()}
    elif isinstance(obj, set):
        return list(deep_serialize(item) for item in obj)
    elif isinstance(obj, type):
        return {
            '__type__': 'type_reference',
            '__name__': obj.__name__,
            '__module__': getattr(obj, '__module__', 'builtins')
        }
    elif isinstance(obj, (datetime, date, time)):
        return {
            '__type__': 'datetime_obj',
            '__class__': obj.__class__.__name__,
            '__value__': obj.isoformat()
        }
    elif callable(obj):
        return {
            '__type__': 'callable_fallback',
            '__value__': str(obj),
            '__name__': getattr(obj, '__name__', 'unknown')
        }
    elif hasattr(obj, '__dict__') or hasattr(obj.__class__, '__slots__'):
        # Handle all custom objects (AST nodes, SQL types, etc.)
        return {
            '__type__': 'ast_object',
            '__class__': obj.__class__.__name__,
            '__module__': obj.__class__.__module__,
            '__slots__': getattr(obj.__class__, '__slots__', None),
            '__data__': deep_serialize_object_data(obj)
        }
    else:
        # Last resort: convert to string
        return {
            '__type__': 'string_fallback',
            '__value__': str(obj)
        }

def deep_serialize_object_data(obj):
    """Extract and deeply serialize all attributes from an object"""
    data = {}
    
    # Handle __slots__ classes
    if hasattr(obj.__class__, '__slots__'):
        for slot in obj.__class__.__slots__:
            if hasattr(obj, slot):
                value = getattr(obj, slot)
                data[slot] = deep_serialize(value)
    
    # Handle regular classes with __dict__
    if hasattr(obj, '__dict__'):
        for key, value in obj.__dict__.items():
            data[key] = deep_serialize(value)
    
    return data

def deep_deserialize(obj):
    """
    Recursively reconstruct objects from serialized data
    """
    if obj is None:
        return None
    elif isinstance(obj, (str, int, float, bool)):
        return obj
    elif isinstance(obj, list):
        return [deep_deserialize(item) for item in obj]
    elif isinstance(obj, dict):
        if '__type__' in obj:
            return deserialize_typed_object(obj)
        else:
            # Regular dictionary
            return {key: deep_deserialize(value) for key, value in obj.items()}
    else:
        return obj

def deserialize_typed_object(obj):
    """Deserialize objects with type information"""
    obj_type = obj['__type__']
    
    if obj_type == 'ast_object':
        return reconstruct_ast_object(obj)
    elif obj_type == 'type_reference':
        return reconstruct_type_reference(obj)
    elif obj_type == 'datetime_obj':
        return reconstruct_datetime_object(obj)
    elif obj_type == 'callable_fallback':
        return obj['__value__']
    elif obj_type == 'string_fallback':
        return obj['__value__']
    else:
        return obj

def reconstruct_ast_object(obj):
    """Reconstruct AST objects like SelectStatement, ColumnExpression"""
    class_name = obj['__class__']
    module_name = obj.get('__module__')
    data = obj['__data__']
    slots = obj.get('__slots__')
    
    # Recursively deserialize the data first
    deserialized_data = deep_deserialize(data)
    
    # Find the class
    cls = find_class(class_name, module_name)
    
    if cls:
        try:
            # Create instance
            instance = cls.__new__(cls)
            
            # Set attributes
            if slots:
                for slot in slots:
                    if slot in deserialized_data:
                        setattr(instance, slot, deserialized_data[slot])
            else:
                for key, value in deserialized_data.items():
                    setattr(instance, key, value)
            
            return instance
            
        except Exception as e:
            print(f"Warning: Could not reconstruct {class_name}: {e}")
            return deserialized_data
    
    return deserialized_data

def find_class(class_name, module_name):
    """Find a class by name, trying multiple strategies"""
    
    # Try specific known classes first
    known_classes = {
        'SelectStatement': 'sql_ast',
        'ColumnExpression': 'sql_ast', 
        'BinaryOperation': 'sql_ast',
        'Function': 'sql_ast',
        'LiteralExpression': 'sql_ast',
        'ConditionExpr': 'sql_ast',
        'OrderBy': 'sql_ast',
        'TableReference': 'sql_ast',
        'MathFunction': 'sql_ast',
        'StringFunction': 'sql_ast',
        'Cast': 'sql_ast',
        'Extract': 'sql_ast',
        'DateDIFF': 'sql_ast',
        'CaseWhen': 'sql_ast',
        'Concat': 'sql_ast',
        'Replace': 'sql_ast',
        'CoalesceFunction': 'sql_ast',
        'NullIF': 'sql_ast',
        'CurrentDate': 'sql_ast',
        'Between': 'sql_ast',
        'Membership': 'sql_ast',
        'IsNullCondition': 'sql_ast',
        'LikeCondition': 'sql_ast',
        'NegationCondition': 'sql_ast'
    }
    
    if class_name in known_classes:
        try:
            module = __import__(known_classes[class_name], fromlist=[class_name])
            return getattr(module, class_name)
        except (ImportError, AttributeError):
            pass
    
    # Try the original module
    if module_name and module_name != '__main__':
        try:
            module = __import__(module_name, fromlist=[class_name])
            return getattr(module, class_name, None)
        except (ImportError, AttributeError):
            pass
    
    # Try finding in loaded modules
    import sys
    for module in sys.modules.values():
        if module and hasattr(module, class_name):
            return getattr(module, class_name)
    
    print(f"Warning: Could not find class {class_name}")
    return None

def reconstruct_type_reference(obj):
    """Reconstruct type references like INT, VARCHAR"""
    type_name = obj['__name__']
    module_name = obj.get('__module__', 'builtins')
    
    try:
        if module_name == 'builtins':
            return getattr(__builtins__, type_name, str)
        else:
            # Check datatypes module
            if 'datatypes' in globals():
                datatypes_dict = globals()['datatypes']
                if hasattr(datatypes_dict, type_name):
                    return getattr(datatypes_dict, type_name)
                elif isinstance(datatypes_dict, dict) and type_name in datatypes_dict:
                    return datatypes_dict[type_name]
            
            # Try importing the module
            module = __import__(module_name, fromlist=[type_name])
            return getattr(module, type_name, str)
    except (ImportError, AttributeError):
        print(f"Warning: Could not find type {type_name}")
        return str

def reconstruct_datetime_object(obj):
    """Reconstruct datetime objects"""
    class_name = obj['__class__']
    iso_value = obj['__value__']
    
    try:
        if class_name == 'datetime':
            return datetime.fromisoformat(iso_value)
        elif class_name == 'date':
            return datetime.fromisoformat(iso_value).date()
        elif class_name == 'time':
            return datetime.fromisoformat(f"1970-01-01T{iso_value}").time()
    except ValueError:
        print(f"Warning: Could not parse datetime {iso_value}")
        return iso_value

serialize_value = deep_serialize
deserialize_value = deep_deserialize