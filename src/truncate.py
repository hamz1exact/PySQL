import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))
from exec.sql_helpers import *

def truncate_table(ast, db_manager):
    table_name = ast.table_name

    temp_view_name = table_name+'._mt_view'
    if temp_view_name in db_manager.views or table_name in db_manager.views:
        raise ValueError ('TRUNCATE is not allowed while working with NORMAL OR MATERILIAZED VIEWS, run DROP VIEW instead')
    elif table_name not in db_manager.active_db:
        raise TableNotFoundError(table_name)
    else:
        db_manager.active_db[table_name].rows = []