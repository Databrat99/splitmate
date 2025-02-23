import pandas as pd
from sqlalchemy import create_engine,text
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# SQL Server typically limits the number of parameters to 2100.
PARAMETER_LIMIT = 2100

def get_engine(server, database):
    engine_url = (
        f"mssql+pyodbc://@{server}/{database}"
        "?driver=ODBC+Driver+17+for+SQL+Server"
    )
    try:
        engine = create_engine(engine_url)
        logging.info(f"Engine created for {database} on {server}.")
        return engine
    except Exception as e:
        logging.error(f"Error creating engine for {database} on {server}: {e}")
        raise

# SQL Server typically limits the number of parameters to 2100.
PARAMETER_LIMIT = 2100


def delete_destination_data(dest_engine, dest_table):
    """
    Deletes data from the destination table.
    """
    try:
        with dest_engine.begin() as conn:
            # Use a SQLAlchemy text() query for deletion.
            # The dest_table should be fully qualified (e.g., "DestSchema.Table1")
            delete_query = text(f"DELETE FROM {dest_table}")
            conn.execute(delete_query)
        logging.info(f"Existing data in {dest_table} deleted successfully.")
    except Exception as e:
        logging.error(f"Error deleting data from {dest_table}: {e}")
        raise

def migrate_table(src_engine, dest_engine, src_table, dest_table, batch_size=20000):
    query = f"SELECT * FROM {src_table}"
    logging.info(f"Starting migration for {src_table} -> {dest_table}")
    
    try:
        df = pd.read_sql(query, src_engine)
    except Exception as e:
        logging.error(f"Error reading from {src_table}: {e}")
        return

    if df.empty:
        logging.warning(f"No data found in {src_table}. Skipping migration.")
        return
    
    try:
        delete_destination_data(dest_engine, dest_table)
    except Exception as e:
        logging.error(f"Aborting migration for {dest_table} due to delete failure.")
        return

    num_cols = len(df.columns)
    safe_batch_size = min(batch_size, max(1, PARAMETER_LIMIT // num_cols))
    if safe_batch_size < batch_size:
        logging.info(f"Adjusted batch size to {safe_batch_size} rows (for {num_cols} columns) to comply with SQL Server's parameter limit.")
    
    # Parse destination table for schema and table name
    if '.' in dest_table:
        schema, table_name = dest_table.split('.', 1)
    else:
        table_name = dest_table
        schema = None

    # Insert data in chunks manually (without using method='multi')
    try:
        for i in range(0, len(df), safe_batch_size):
            df_chunk = df.iloc[i:i+safe_batch_size]
            df_chunk.to_sql(table_name, dest_engine, schema=schema, if_exists='append', index=False, chunksize=safe_batch_size)
            logging.info(f"Inserted rows {i} to {i+len(df_chunk)} into {dest_table}.")
        logging.info(f"Successfully migrated {src_table} to {dest_table}.")
    except Exception as e:
        logging.error(f"Error writing to {dest_table}: {e}")

def migrate_multiple_tables(src_engine, dest_engine, table_mappings, batch_size=20000):
    for src_table, dest_table in table_mappings.items():
        migrate_table(src_engine, dest_engine, src_table, dest_table, batch_size)

if __name__ == "__main__":
    try:
        src_engine = get_engine("Vishglory", "DW")
        dest_engine = get_engine("Vishglory", "Movies")
    except Exception as e:
        logging.critical("Failed to create database engines. Exiting.")
        exit(1)
    
    table_mappings = {
        "dbo.Ptest2": "dbo.Ptest2",
        "dbo.PTest": "dbo.PTest",
        "dsa.tblEmp": "dsa.tblEmp",
        "dbo.Ptest4": "dbo.Ptest4",
        "dbo.Ptest5": "dbo.Ptest5"
        # Add additional table mappings as needed...
    }
    
    try:
        migrate_multiple_tables(src_engine, dest_engine, table_mappings, batch_size=10000)
    except Exception as e:
        logging.error(f"Unexpected error during migration: {e}")
    finally:
        src_engine.dispose()
        dest_engine.dispose()
        logging.info("Database engines disposed. Migration process completed.")
-----------------------------------------------------------------------------------------

import pandas as pd
from sqlalchemy import create_engine

def get_engine(server, database, username, password):
    engine_url = (
        f"mssql+pyodbc://{username}:{password}@{server}/{database}"
        "?driver=ODBC+Driver+17+for+SQL+Server"
    )
    return create_engine(engine_url)

def migrate_table(src_engine, dest_engine, src_table, dest_table, batch_size=10000):
    """
    Migrate data from the source table to the destination table.
    """
    query = f"SELECT * FROM {src_table}"
    print(f"Starting migration for {src_table} -> {dest_table}")
    df = pd.read_sql(query, src_engine)
    
    if df.empty:
        print(f"No data found in {src_table}. Skipping.")
    else:
        # Extract table name from fully-qualified destination schema.table
        dest_table_name = dest_table.split('.')[-1]
        # Use chunksize and multi-row insert for improved performance
        df.to_sql(dest_table_name, dest_engine, if_exists='append', index=False,
                  method='multi', chunksize=batch_size)
        print(f"Successfully migrated {src_table} to {dest_table}")

def migrate_multiple_tables(src_engine, dest_engine, table_mappings, batch_size=10000):
    for src_table, dest_table in table_mappings.items():
        migrate_table(src_engine, dest_engine, src_table, dest_table, batch_size)

if __name__ == "__main__":
    # Configure source and destination SQL Server connections.
    src_engine = get_engine("SourceServer", "SourceDB", "username", "password")
    dest_engine = get_engine("DestServer", "DestDB", "username", "password")
    
    # Define mappings between source and destination tables (with different schemas)
    table_mappings = {
        "SourceSchema.Table1": "DestSchema.Table1",
        "SourceSchema.Table2": "DestSchema.Table2",
        "SourceSchema.Table3": "DestSchema.Table3",
        # Add additional table mappings as needed...
    }
    
    # Migrate the tables.
    migrate_multiple_tables(src_engine, dest_engine, table_mappings, batch_size=10000)
    
    # Clean up connections.
    src_engine.dispose()
    dest_engine.dispose()
