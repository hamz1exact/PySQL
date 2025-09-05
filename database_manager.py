import os
import platform
import msgpack

class Table:
    def __init__(self, name, schema, defaults=None, auto=None):
        self.name = name
        self.schema = schema
        self.defaults = defaults or {}
        self.auto = auto or {}
        self.rows = []

class DatabaseManager:
    def __init__(self):
        home = os.path.expanduser("~")

        if platform.system() == "Windows":
            # Example: C:\Users\<User>\AppData\Roaming\su_sql
            self.db_folder = os.path.join(os.getenv("APPDATA"), "su_sql")
        else:
            # Example: ~/.su_sql
            self.db_folder = os.path.join(home, ".su_sql")

        # Hidden cache file inside db folder
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
            print(f"Auto-connecting to last used database: {self.active_db_name}")
            self.load_database_file()

    # ---------------- Database Operations ----------------
    def create_database(self, db_name):
        db_file = os.path.join(self.db_folder, f"{db_name}.su")
        if os.path.exists(db_file):
            raise ValueError(f"Database '{db_name}' already exists")
        self.databases.append(db_file)
        self.active_db_name = db_file
        with open(db_file, "wb") as f:
            msgpack.pack({}, f)  # empty DB
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
            table = Table(
                name=tbl_name,
                schema=tbl_dict.get("schema", {}),
                defaults=tbl_dict.get("defaults", {}),
                auto=tbl_dict.get("auto", {})
            )
            table.rows = tbl_dict.get("rows", [])
            self.active_db[tbl_name] = table

    def save_database_file(self):
        db_data = {}
        for tbl_name, table in self.active_db.items():
            db_data[tbl_name] = {
                "schema": table.schema,
                "defaults": table.defaults,
                "auto": table.auto,
                "rows": table.rows
            }
        with open(self.active_db_name, "wb") as f:
            msgpack.pack(db_data, f)