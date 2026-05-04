import duckdb as db
from src.config.paths import (SUBCATEGORIES_DDL_PATH, SUBCATEGORIES_CSV_PATH, SUBCATEGORY_RAW_PATH, SUBCATEGORIES_PARQUET_PATH)

def generate_subcategories(conn):
        create_db = SUBCATEGORIES_DDL_PATH.read_text()

        conn.execute(create_db)

        conn.execute(f'''
                INSERT INTO DIM_SUBCATEGORY (SELECT * FROM read_csv_auto('{SUBCATEGORY_RAW_PATH}'))
            ''')
        
        conn.execute(f'''
                    COPY DIM_SUBCATEGORY TO '{SUBCATEGORIES_PARQUET_PATH}' (FORMAT PARQUET)
''')
