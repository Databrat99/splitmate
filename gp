import pandas as pd
from sqlalchemy import create_engine
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_engine(server, database, username, password):
    engine_url = (
        f"mssql+pyodbc://{username}:{password}@{server}/{database}"
        "?driver=ODBC+Driver+17+for+SQL+Server"
    )
    try:
        engine = create_engine(engine_url)
        logging.info(f"Engine created for {database} on {server}.")
        return engine
    except Exception as e:
        logging.error(f"Error creating engine for {database} on {server}: {e}")
        raise

def migrate_table(src_engine, dest_engine, src_table, dest_table, batch_size=10000):
    try:
        query = f"SELECT * FROM {src_table}"
        logging.info(f"Starting migration for {src_table} -> {dest_table}")
        df = pd.read_sql(query, src_engine)
    except Exception as e:
        logging.error(f"Error reading data from {src_table}: {e}")
        return

    if df.empty:
        logging.warning(f"No data found in {src_table}. Skipping migration.")
        return

    try:
        dest_table_name = dest_table.split('.')[-1]
        df.to_sql(dest_table_name, dest_engine, if_exists='append', index=False,
                  method='multi', chunksize=batch_size)
        logging.info(f"Successfully migrated {src_table} to {dest_table}.")
    except Exception as e:
        logging.error(f"Error writing data to {dest_table}: {e}")

def migrate_multiple_tables(src_engine, dest_engine, table_mappings, batch_size=10000):
    for src_table, dest_table in table_mappings.items():
        try:
            migrate_table(src_engine, dest_engine, src_table, dest_table, batch_size)
        except Exception as e:
            logging.error(f"Unexpected error migrating {src_table}: {e}")

if __name__ == "__main__":
    try:
        src_engine = get_engine("SourceServer", "SourceDB", "username", "password")
        dest_engine = get_engine("DestServer", "DestDB", "username", "password")
    except Exception as e:
        logging.critical("Failed to create database engines. Exiting.")
        exit(1)
    
    # Define mappings between source and destination tables (with different schemas)
    table_mappings = {
        "SourceSchema.Table1": "DestSchema.Table1",
        "SourceSchema.Table2": "DestSchema.Table2",
        "SourceSchema.Table3": "DestSchema.Table3",
        "SourceSchema.Table4": "DestSchema.Table4",
        "SourceSchema.Table5": "DestSchema.Table5",
        "SourceSchema.Table6": "DestSchema.Table6",
        "SourceSchema.Table7": "DestSchema.Table7",
        "SourceSchema.Table8": "DestSchema.Table8",
        "SourceSchema.Table9": "DestSchema.Table9",
        "SourceSchema.Table10": "DestSchema.Table10",
        "SourceSchema.Table11": "DestSchema.Table11",
        "SourceSchema.Table12": "DestSchema.Table12",
        "SourceSchema.Table13": "DestSchema.Table13",
        "SourceSchema.Table14": "DestSchema.Table14",
        "SourceSchema.Table15": "DestSchema.Table15",
        "SourceSchema.Table16": "DestSchema.Table16",
        "SourceSchema.Table17": "DestSchema.Table17",
        "SourceSchema.Table18": "DestSchema.Table18",
        "SourceSchema.Table19": "DestSchema.Table19",
        "SourceSchema.Table20": "DestSchema.Table20",
        "SourceSchema.Table21": "DestSchema.Table21",
        "SourceSchema.Table22": "DestSchema.Table22",
        "SourceSchema.Table23": "DestSchema.Table23",
        "SourceSchema.Table24": "DestSchema.Table24",
        "SourceSchema.Table25": "DestSchema.Table25",
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
