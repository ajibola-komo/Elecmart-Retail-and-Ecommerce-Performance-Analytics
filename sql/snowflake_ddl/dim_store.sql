CREATE OR REPLACE TABLE dim_store(
    store_id INT PRIMARY KEY,
    store_name VARCHAR NOT NULL,
    location_id INT,
    store_type VARCHAR,
    store_size INT,
    opening_date TIMESTAMP_NTZ,
    opening_date_id INT,
    foot_traffic_index INT,
    foreign key(location_id) references dim_location(location_id),
    foreign key(opening_date_id) references dim_date(date_id)
);