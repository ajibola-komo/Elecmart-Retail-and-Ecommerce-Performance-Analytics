CREATE OR REPLACE TABLE dim_campaign(
    campaign_id INT PRIMARY KEY,
    campaign_name VARCHAR,
    campaign_channel VARCHAR,
    promo_id INT,
    campaign_start_date TIMESTAMP_NTZ,
    campaign_start_date_id INT,
    campaign_end_date TIMESTAMP_NTZ,
    campaign_end_date_id INT,
    FOREIGN KEY(promo_id) references dim_PROMOTION(promo_id),
    FOREIGN KEY(campaign_start_date_id) references DIM_DATE(date_id),
    FOREIGN KEY(campaign_end_date_id) references DIM_DATE(date_id)
);