from src.config.paths import(LOCATIONS_DDL_PATH, LOCATION_CSV_PATH, LOCATION_PARQUET_PATH)
from src.config.constants import(FOOT_TRAFFIC, PROVINCE_CITY_MAP)



def generate_locations(conn):

        location_id = 0


        create_db = LOCATIONS_DDL_PATH.read_text()

        conn.execute(create_db)

        conn.execute(f"DELETE FROM dim_location")

        rows = []

        for state_province, data in PROVINCE_CITY_MAP.items():
            cities = data['cities']
            location_types = data['location_type']
            location_weights = data['location_weights']
            

            for city, location_weight, location_type in zip(cities, location_weights, location_types):
                location_id += 1
                foot_min, foot_max = FOOT_TRAFFIC[location_type]
                rows.append((
                    location_id,
                    "Canada",
                    state_province,
                    city,
                    location_type,
                    location_weight,
                    foot_min,
                    foot_max
                ))

        conn.executemany('''
                INSERT INTO dim_location (location_id, country, state_province,city, location_type, location_weight, 
                         foot_traffic_min, foot_traffic_max) VALUES (?,?,?,?,?,?,?,?)
            ''',rows)

        conn.execute(f'''
                    COPY dim_location TO '{LOCATION_PARQUET_PATH}' (FORMAT PARQUET)
''')


        