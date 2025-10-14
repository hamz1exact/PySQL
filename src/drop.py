import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))
from exec.sql_helpers import *

def drop_database(ast, db_manager):
    import os
    file_path = get_databse_path(ast.database_name, db_manager)
    if file_path is not None:
        os.remove(file_path)
        db_manager.databases.remove(file_path)
    else:
        raise ValueError(f'No Database With name {ast.database_name}')
    
    
    
def get_databse_path(db_name ,database):
    import os
    
    for db_file in database.databases:
        filename = os.path.basename(db_file)
        if filename.endswith('.su'):
            filename = filename[:-3]
            if db_name == filename:
                if db_file == database.active_db_name:
                    raise ValueError('You Cannot Delete a Database While You are using it, Please Switch to another Database Before doing this action')
                return db_file
    return None
        
        
def drop_table(ast, database):
    
    if ast.table_name not in database.active_db:
        raise TableNotFoundError(ast.table_name)
    elif str(ast.table_name)+'._mt_view' in database.views:
        raise ValueError('You are trying to Drop a MATERIALIZED VIEW, use DROP MATERIALIZED VIEW <view_name> instead')
    del database.active_db[ast.table_name]
    
    
    
    

def drop_view(ast, db_manager):
    if ast.view_name not in db_manager.views:
        raise ValueError(f"View {ast.view_name} not found")
    elif ast.view_name.endswith("_mt_view"):
        raise ValueError('That View Belongs to Materialized View, You Cannot Drop It Unless You Call DROP MATERIALIZED VIEW <view_name>')
    else:
        del db_manager.views[ast.view_name]
        
def drop_materialized_view(ast, database):
    if ast.view_name in database.active_db and str(ast.view_name)+"._mt_view" in database.views:
        del database.active_db[ast.view_name]
        del database.views[str(ast.view_name)+"._mt_view"]
    else:
        raise ValueError(f"View {ast.view_name} not found")
    