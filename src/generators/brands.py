from src.config.paths import (BRAND_RAW_PATH, BRANDS_DDL_PATH, BRANDS_CSV_PATH, BRANDS_PARQUET_PATH)

def generate_brands(conn):
        create_db = BRANDS_DDL_PATH.read_text()
        conn.execute(create_db)

        conn.execute(f'''
                    INSERT INTO DIM_BRAND SELECT * FROM read_csv_auto('{BRAND_RAW_PATH}')
                ''')
        
        conn.execute(f'''
                    COPY DIM_BRAND TO '{BRANDS_CSV_PATH}' (FORMAT CSV, HEADER true)
                ''')
        
        conn.execute(f'''
                    COPY DIM_BRAND TO '{BRANDS_PARQUET_PATH}' (FORMAT PARQUET)
                ''')


