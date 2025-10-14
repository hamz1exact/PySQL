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
        