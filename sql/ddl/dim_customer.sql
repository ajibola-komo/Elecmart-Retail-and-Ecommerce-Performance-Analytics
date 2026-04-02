CREATE TABLE dim_customer (
    customer_id INT PRIMARY KEY,  
    email_address VARCHAR UNIQUE NOT NULL,
    first_name VARCHAR NOT NULL,
    last_name VARCHAR NOT NULL,
    gender VARCHAR,
    customer_persona VARCHAR,
    birth_date date,
    birth_year INT,
    location_id int,
    signup_date TIMESTAMP,
    signup_date_id int,
    signup_channel VARCHAR,
    loyalty_status VARCHAR,
    estimated_annual_income DECIMAL(10,2),
    email_opt_in BOOLEAN,
    sms_opt_in BOOLEAN
);
