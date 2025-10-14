import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))
import os
import platform
import msgpack
# from .datatypes import datatypes, SERIAL # assuming Lexer.datatypes contains your SQLType classes
from sql_types.sql_types import datatypes, SERIAL
import random
from storage.table import *
from storage.serialize import *
from storage.deserialize import *
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
                











serialize_value = deep_serialize
deserialize_value = deep_deserialize