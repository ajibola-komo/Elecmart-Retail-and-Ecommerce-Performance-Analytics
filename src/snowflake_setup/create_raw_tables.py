import snowflake.connector
import os
from dotenv import load_dotenv
from src.config.envariables import SNOWFLAKE_CONFIG
from src.config.paths import SNOWFLAKE_DDL_DIR, TABLE_NAMES

load_dotenv()

def create_snowflake_bronze_tables():
    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    cursor = conn.cursor()
    

    database_name = SNOWFLAKE_CONFIG.get("database")
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {database_name}")


    schema_name = SNOWFLAKE_CONFIG.get("schema")
    cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {database_name}.{schema_name}")

    cursor.execute(f"USE DATABASE {database_name}")
    cursor.execute(f"USE SCHEMA {database_name}.{schema_name}")

    for table_name in TABLE_NAMES:
        sql_path = SNOWFLAKE_DDL_DIR / f"{table_name}.sql"
        with open(sql_path, "r") as f:
            ddl = f.read()
        
        cursor.execute(ddl)

    cursor.close()
    conn.close()