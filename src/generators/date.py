import duckdb as db
from src.config.paths import (DATE_DDL_PATH, DATES_CSV_PATH, DATES_PARQUET_PATH)

def generate_dates(conn):

    create_db = DATE_DDL_PATH.read_text()

    conn.execute(create_db)

    conn.execute(f'''
                    COPY dim_date TO '{DATES_PARQUET_PATH}' (FORMAT PARQUET)
''')