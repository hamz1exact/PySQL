import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))
from exec.sql_helpers import *



def create_new_view(ast, db_manager):
    if ast.view_name in db_manager.views and not ast.can_be_replaced:
        print(f"Error: View '{ast.view_name}' already exists")
    else:
        db_manager.views[ast.view_name] = ast.query
        print(f"View '{ast.view_name}' created successfully")
        db_manager.save_database_file()
        
def execute_create_table_statement(ast, database):

        table = Table(ast.table_name, ast.schema, ast.defaults, ast.auto, ast.constraints, ast.restrictions, ast.private_constraints, ast.constraints_ptr)
        if ast.table_name in database.active_db:
            raise ValueError('Table Already Exists')
        database.active_db[ast.table_name] = table
        

def execute_create_database_statement(ast, database):
        database.create_database(ast.database_name)

        database.use_database(ast.database_name)