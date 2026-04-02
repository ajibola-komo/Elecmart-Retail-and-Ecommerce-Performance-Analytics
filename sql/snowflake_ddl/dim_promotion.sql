CREATE OR REPLACE TABLE dim_promotion(
    promo_id INT primary key,
    promo_name VARCHAR,
    promo_type VARCHAR,
    discount_type VARCHAR,
    discount_value DECIMAL(10,2),
    promo_start_date TIMESTAMP_NTZ,
    promo_start_date_id INT,
    promo_end_date TIMESTAMP_NTZ,
    promo_end_date_id INT,
    promo_duration INT,
    promo_code VARCHAR,
    is_active BOOLEAN,
    promo_description VARCHAR,
    FOREIGN KEY(promo_start_date_id) references dim_date(date_id),
    FOREIGN KEY(promo_end_date_id) references dim_date(date_id)
);