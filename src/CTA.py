import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))
from exec.sql_helpers import *

def execute_CTA(ast, database):
    # Execute the query to get results
    results = ast.query.evaluate()
    
    if not results:
        raise ValueError("Cannot create table from empty query result")
    
    # Analyze ALL rows to determine proper schema (not just first row)
    schema = {}
    column_samples = {}
    
    # Collect samples from all rows to better determine types
    for row in results:
        for col, val in row.items():
            if col not in column_samples:
                column_samples[col] = []
            if val is not None:  # Only collect non-null samples
                column_samples[col].append(val)
    
    # Determine schema based on samples
    for col, samples in column_samples.items():
        if not samples:  # All values were None
            schema[col] = VARCHAR
            continue
            
        # Check the first few non-null values to determine type
        sample_val = samples[0]
        
        # Important: Check bool BEFORE int because isinstance(True, int) returns True in Python!
        if isinstance(sample_val, bool):
            schema[col] = BOOLEAN
        elif isinstance(sample_val, int):
            schema[col] = INT
        elif isinstance(sample_val, float):
            schema[col] = FLOAT
        elif isinstance(sample_val, str):
            schema[col] = VARCHAR
        elif isinstance(sample_val, datetime):
            schema[col] = TIMESTAMP
        elif isinstance(sample_val, date):
            schema[col] = DATE
        elif isinstance(sample_val, time):
            schema[col] = TIME
        else:
            schema[col] = VARCHAR
            print(f"Warning: Unknown type {type(sample_val)} for column '{col}', defaulting to VARCHAR")
    
    # Handle columns that had no samples (all NULL)
    if not schema:
        raise ValueError("Cannot determine schema: all columns contain only NULL values")
    
    if ast.table_name in database.active_db:
        raise ValueError(f"Table '{ast.table_name}' already exists")
    
    new_table = Table(
        name=ast.table_name, 
        schema=schema, 
        defaults={}, 
        auto={}, 
        constraints={}, 
        restrictions={}, 
        private_constraints={}, 
        constraints_ptr={}
    )
    
    database.active_db[ast.table_name] = new_table
    current_table = database.active_db[ast.table_name]
    
    if ast.with_data:
        for row in results:
            # Convert raw values to SQLType objects before storing
            converted_row = {}
            for col, val in row.items():
                if val is None:
                    converted_row[col] = None
                else:
                    try:
                        # Use the schema to create proper SQLType objects
                        sql_type_class = schema[col]
                        converted_row[col] = sql_type_class(val)
                    except Exception as e:
                        print(f"Error converting value {val} ({type(val)}) to {sql_type_class.__name__} for column '{col}': {e}")
                        # Fallback: convert to VARCHAR if type conversion fails
                        converted_row[col] = VARCHAR(str(val))
            
            current_table.rows.append(converted_row)
    
    print(f"Table '{ast.table_name}' created with {len(current_table.rows)} rows")
    
    # Debug: Show the inferred schema
    print("Inferred schema:")
    for col, sql_type in schema.items():
        print(f"  {col}: {sql_type.__name__}")