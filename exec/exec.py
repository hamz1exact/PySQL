import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))
from engine.sql_ast import *
from exec.sql_helpers import *
from engine.sql_ast import *
from storage.database_manager import Table
from sql_types.sql_types import *
from errors import *
from src.select import execute_select_query
from src.insert import execute_insert_query
from src.update import *
from src.delete import *
from src.drop import *
from src.create import *
from src.truncate import *
from src.CTE import *
from src.CTA import *
from src.use import *
from src.call_view import *
from src.materialized_view import *


def execute(ast, database):
    if isinstance(ast, SelectStatement):
        return execute_select_query(ast, database)
    
    elif isinstance(ast, InsertStatement):
        return execute_insert_query(ast, database)
        
    elif isinstance(ast, UpdateStatement):
        return execute_update_query(ast, database)
        
    elif isinstance(ast, DeleteStatement):
        return execute_delete_query(ast, database)
        
    elif isinstance(ast, CreateDatabseStatement):
        return execute_create_database_statement(ast, database)
        
    elif isinstance(ast, CreateTableStatement):
        return execute_create_table_statement(ast, database)
        
    elif isinstance(ast, UseStatement):
        return execute_use_statement(ast, database)
    
    elif isinstance(ast, CreateView):
        return create_new_view(ast, database)
    
    elif isinstance(ast, CallView):
        return call_view(ast, database)
    
    elif isinstance(ast, CTA):
        return execute_CTA(ast, database)
    
    elif isinstance(ast, CreateMaterializedView):
        return create_materialized_view(ast, database)
    
    elif isinstance(ast, RefreshMaterializedView):
        return refresh_meterialized_view(ast, database)
    
    elif isinstance(ast, DropDatabase):
        return drop_database(ast, database)
    
    elif isinstance(ast, DropTable):
        return drop_table(ast, database)
    
    elif isinstance(ast, DropView):
        return drop_view(ast, database)
    
    elif isinstance(ast, DropMTView):
        return drop_materialized_view(ast, database)
    
    elif isinstance(ast, TruncateTable):
        return truncate_table(ast, database)

def create_new_view(ast, db_manager):
    if ast.view_name in db_manager.views and not ast.can_be_replaced:
        print(f"Error: View '{ast.view_name}' already exists")
    else:
        db_manager.views[ast.view_name] = ast.query
        print(f"View '{ast.view_name}' created successfully")
        db_manager.save_database_file()

# Replace your execute_CTA function with this fixed version:



    
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
    

        
        
def create_temp_cte_table(ast, db_manager):
    cte_rows = ast.query.evaluate()
    cte_schema = generate_schema(cte_rows)
    new_table = Table(
        name=ast.table_name, 
        schema=cte_schema, 
        defaults={}, 
        auto={}, 
        constraints={}, 
        restrictions={}, 
        private_constraints={}, 
        constraints_ptr={}
    )
    for row in cte_rows:
        converted_rows = {}
        for col, val in row.items():
            if val is None:
                converted_rows[col] = None
            else:
                try:
                    sql_type_class = cte_schema[col]
                    converted_rows[col] = sql_type_class(val)
                except Exception as e:
                    print(f"Error converting value {val} ({type(val)}) to {sql_type_class.__name__} for column '{col}': {e}")
                    converted_rows[col] = VARCHAR(str(val))
        new_table.rows.append(converted_rows)
    db_manager.active_db[ast.cte_name] = new_table
                    
