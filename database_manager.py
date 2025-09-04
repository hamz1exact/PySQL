
import msgpack
import os
class Table:
    def __init__(self, name, schema, defaults=None, auto=None):
        self.name = name
        self.schema = schema            # column_name -> datatype
        self.defaults = defaults or {}  # default values for non-AUTO_INT columns
        self.auto = auto or {}          # AUTO_INT counters
        self.rows = []                  # list of row dicts
        self.indexes = {}               # optional for performance

# ------------------- DatabaseManager Class -------------------
class DatabaseManager:
    def __init__(self, db_folder="databases", cache_file=".su_cache"):
        self.db_folder = db_folder
        self.cache_file = cache_file
        self.databases = []          # list of all DB file paths
        self.active_db_name = None   # currently active DB file path
        self.active_db = {}          # in-memory tables
        os.makedirs(db_folder, exist_ok=True)
        self.load_cache()
        self.auto_use_recent_db()

    # ----------------- Cache Handling -----------------
    


    def load_cache(self):
        if os.path.exists(self.cache_file):
            try:
                with open(self.cache_file, "rb") as f:
                    cache = msgpack.unpack(f)
                self.databases = cache.get("databases", [])
                self.recent = cache.get("recent", None)
            except Exception:
                self.databases = []
                self.recent = None
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

    # ---------------- Auto-use recent DB ----------------
    def auto_use_recent_db(self):
        if hasattr(self, "recent") and self.recent and os.path.exists(self.recent):
            self.active_db_name = self.recent
            print(f"Auto-connecting to last used database: {self.active_db_name}")
            self.load_database_file()

    # ----------------- Database Operations -----------------
    def create_database(self, db_name):
        db_file = os.path.join(self.db_folder, f"{db_name}.su")
        if os.path.exists(db_file):
            raise ValueError(f"Database '{db_name}' already exists")
        self.databases.append(db_file)
        self.active_db_name = db_file  # automatically use it
        # initialize empty database file
        with open(db_file, "wb") as f:
            msgpack.pack({}, f)
        self.update_cache()
        print(f"Database '{db_name}' created and selected")

    def use_database(self, db_name):
        db_file = os.path.join(self.db_folder, f"{db_name}.su")
        if db_file not in self.databases:
            raise ValueError(f"Database '{db_name}' does not exist")
        # self.active_db = db_name
        self.active_db_name = db_file
        self.load_database_file()
        self.update_cache()
        print(f"Now using database '{db_name}'")
    # ----------------- Table Loading/Saving -----------------
    def load_database_file(self):
        """Load tables from file into memory."""
        if self.active_db_name and os.path.exists(self.active_db_name):
            with open(self.active_db_name, "rb") as f:
                db_data = msgpack.unpack(f)
            self.active_db = {}
            for tbl_name, tbl_dict in db_data.items():
                table = Table(
                    name=tbl_name,
                    schema=tbl_dict.get("schema", {}),
                    defaults=tbl_dict.get("defaults", {}),
                    auto=tbl_dict.get("auto", {}),
                )
                table.rows = tbl_dict.get("rows", [])
                self.active_db[tbl_name] = table
        else:
            self.active_db = {}


    def save_database_file(self):
        """Save all tables back to file."""
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

# dbm = DatabaseManager()
# dbm.create_database("mydb")
# dbm.use_database("mydb")

# # Create a table
# table = Table(
#     name="users",
#     schema={"id": "AUTO_INT", "name": "CSTR", "age": "INT"},
#     defaults={"name": "", "age": 0},
#     auto={"id": 0}
# )
# dbm.active_db["users"] = table
# dbm.save_database_file()

# # Later, load the DB again
# dbm.use_database("mydb")
# print(dbm.active_db["users"].schema)
