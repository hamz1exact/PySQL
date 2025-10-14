import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))
from exec.sql_helpers import *

def execute_use_statement(ast, database):
        database.use_database(ast.database_name)
        database.save_database_file()