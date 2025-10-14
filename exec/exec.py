import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))
from engine.sql_ast import *
from exec.sql_helpers import *
from engine.sql_ast import *
from storage.database import Table
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
