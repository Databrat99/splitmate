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


#################################################################


import pandas as pd
import keyring
from sqlalchemy import create_engine, text
import logging
from datetime import datetime

# Configure logging to include time, log level, and message.
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_source_engine(dsn, username, password):
    """
    Create a SQLAlchemy engine for the source using DSN with SQL Server authentication.
    """
    connection_string = (
        f"mssql+pyodbc://{username}:{password}@{dsn}"
        "?driver=ODBC+Driver+17+for+SQL+Server;Trusted_Connection=no"
    )
    return create_engine(connection_string, connect_args={'fast_executemany': True})

def get_target_engine(dsn):
    """
    Create a SQLAlchemy engine for the target using DSN with Windows authentication.
    """
    # When using Windows authentication, omit username and password and use Trusted_Connection=yes.
    connection_string = (
        f"mssql+pyodbc://@{dsn}"
        "?driver=ODBC+Driver+17+for+SQL+Server;Trusted_Connection=yes"
    )
    return create_engine(connection_string, connect_args={'fast_executemany': True})

def delete_target_data(engine, target_table):
    """
    Delete all rows from the target table.
    target_table should be fully qualified (e.g., "TargetSchema.Table1").
    """
    try:
        with engine.begin() as conn:
            conn.execute(text(f"DELETE FROM {target_table}"))
        logging.info(f"Deleted existing data from {target_table}.")
    except Exception as e:
        logging.error(f"Error deleting data from {target_table}: {e}")
        raise

def migrate_table(source_engine, target_engine, source_table, target_table, chunksize=10000):
    """
    Migrate data from a source table into a target table:
      1. Delete existing data in the target.
      2. Read source data in chunks.
      3. Insert each chunk into the target.
    
    Both source_table and target_table should be fully qualified 
    (e.g., "SourceDB.dbo.Table1" and "TargetSchema.Table1").
    """
    logging.info(f"Starting migration from {source_table} to {target_table}.")

    # 1. Delete existing data in the target table.
    try:
        delete_target_data(target_engine, target_table)
    except Exception:
        logging.error(f"Aborting migration for {target_table} due to delete failure.")
        return

    query = f"SELECT * FROM {source_table}"
    total_rows = 0
    start_time = datetime.now()

    # Split target table into schema and table name if provided.
    if '.' in target_table:
        schema, table_name = target_table.split('.', 1)
    else:
        schema = None
        table_name = target_table

    # 2. Read source data in chunks and insert each chunk.
    try:
        for chunk in pd.read_sql(query, source_engine, chunksize=chunksize):
            total_rows += len(chunk)
            chunk.to_sql(table_name, target_engine, schema=schema, if_exists='append',
                         index=False)
            logging.info(f"Inserted chunk of {len(chunk)} rows into {target_table}.")
    except Exception as e:
        logging.error(f"Error migrating data from {source_table} to {target_table}: {e}")
        return

    elapsed = datetime.now() - start_time
    logging.info(f"Completed migration for {source_table} -> {target_table}. "
                 f"Total rows: {total_rows}. Time taken: {elapsed.total_seconds():.2f} seconds.")

def migrate_all_tables(source_engine, target_engine, table_mappings, chunksize=10000):
    """
    Iterate over table mappings (source: target) and migrate each table.
    """
    for source_table, target_table in table_mappings.items():
        migrate_table(source_engine, target_engine, source_table, target_table, chunksize)

if __name__ == "__main__":
    # DSN names for source and target SQL Server instances.
    source_dsn = "SourceDSN"  # Replace with your source DSN name
    target_dsn = "TargetDSN"  # Replace with your target DSN name

    # Source SQL Server username for SQL Server authentication.
    username = "your_username"  # Replace with your source SQL Server username

    # Retrieve the password securely from keyring.
    password = keyring.get_password("SQLServer", username)
    if not password:
        raise ValueError("Password not found in keyring for service 'SQLServer' and the given username.")

    # Create SQLAlchemy engines for source (DSN with SQL auth) and target (DSN with Windows auth).
    source_engine = get_source_engine(source_dsn, username, password)
    target_engine = get_target_engine(target_dsn)

    # Define table mappings (source: target). Update these as needed.
    table_mappings = {
        "SourceDB.dbo.Table1": "TargetSchema.Table1",
        "SourceDB.dbo.Table2": "TargetSchema.Table2",
        "SourceDB.dbo.Table3": "TargetSchema.Table3",
        # Add additional mappings as required...
    }

    # Migrate each table using chunked processing.
    migrate_all_tables(source_engine, target_engine, table_mappings, chunksize=10000)

    # Clean up by disposing of the engines.
    source_engine.dispose()
    target_engine.dispose()
    logging.info("All migrations completed.")

