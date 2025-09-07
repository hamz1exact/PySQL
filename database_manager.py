import os
import platform
import msgpack
from datatypes import datatypes, SERIAL  # assuming Lexer.datatypes contains your SQLType classes

class Table:
    def __init__(self, name, schema, defaults=None, auto=None):
        self.name = name
        self.schema = schema                  # dict[col_name] = SQLType class
        self.defaults = defaults or {}        # dict[col_name] = SQLType instance
        self.auto = auto or {}                # dict[col_name] = SQLType instance (SERIAL)
        self.rows = []                       # list of dicts with parsed Python values

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
        with open(self.active_db_name, "rb") as f:
            db_data = msgpack.unpack(f)

        self.active_db = {}
        for tbl_name, tbl_dict in db_data.items():
            # Reconstruct schema classes
            schema = {col: datatypes[tbl_dict["schema"][col]] for col in tbl_dict["schema"]}
            
            # Reconstruct defaults using SQLType parse
            defaults = {col: schema[col](tbl_dict["defaults"][col]) for col in tbl_dict.get("defaults", {})}

            # Reconstruct auto values
            auto = {col: schema[col](tbl_dict["auto"][col]) for col in tbl_dict.get("auto", {})}

            # Reconstruct rows
            rows = []
            for row_dict in tbl_dict.get("rows", []):
                row = {col: schema[col](row_dict[col]) for col in row_dict}
                rows.append(row)

            table = Table(tbl_name, schema, defaults, auto)
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

    def save_database_file(self):
        db_data = {}

        def serialize_value(val):
            from datetime import date, time
            # If val is a SQLType instance, get its inner value
            if hasattr(val, "value"):
                val = val.value
            # Serialize date and time objects to strings
            if isinstance(val, date):
                return val.isoformat()  # YYYY-MM-DD
            if isinstance(val, time):
                return val.strftime("%H:%M:%S")  # HH:MM:SS
            return val

        for tbl_name, table in self.active_db.items():
            db_data[tbl_name] = {
                # Save class names for schema
                "schema": {col: table.schema[col].__name__ for col in table.schema},
                # Serialize default and auto values
                "defaults": {col: serialize_value(table.defaults[col]) for col in table.defaults},
                "auto": {col: serialize_value(table.auto[col]) for col in table.auto},
                "rows": [
                    {col: serialize_value(row[col]) for col in row}
                    for row in table.rows
                ]
            }

        with open(self.active_db_name, "wb") as f:
            msgpack.pack(db_data, f)