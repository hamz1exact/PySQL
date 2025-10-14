import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))
from exec.sql_helpers import *

def call_view(ast, database):
    if ast.view_name not in database.views:
        raise ValueError (f"There is no view with name <{ast.view_name}>")
    return database.views[ast.view_name]