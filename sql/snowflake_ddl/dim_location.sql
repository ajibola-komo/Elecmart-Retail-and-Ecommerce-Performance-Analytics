CREATE OR REPLACE TABLE dim_location(
    location_id int primary key,
    country varchar,
    state_province varchar,
    city varchar,
    location_type varchar,
    location_weight decimal(10,2),
    foot_traffic_min int,
    foot_traffic_max int
);