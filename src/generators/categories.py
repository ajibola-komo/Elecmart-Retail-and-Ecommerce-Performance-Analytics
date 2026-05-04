import duckdb as db
from src.config.paths import (CATEGORY_RAW_PATH, CATEGORIES_DDL_PATH, CATEGORIES_CSV_PATH, CATEGORIES_PARQUET_PATH)

def generate_categories(conn):
        create_db = CATEGORIES_DDL_PATH.read_text()

        conn.execute(create_db)

        conn.execute('DELETE FROM DIM_CATEGORY')

        conn.execute(f'''
                INSERT INTO DIM_CATEGORY (SELECT * FROM read_csv_auto('{CATEGORY_RAW_PATH}'))
            ''')
        
        conn.execute(f'''
                    COPY DIM_CATEGORY TO '{CATEGORIES_PARQUET_PATH}' (FORMAT PARQUET)
''')