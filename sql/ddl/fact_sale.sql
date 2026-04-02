CREATE TABLE fact_sale (
    sale_id INT PRIMARY KEY,
    transaction_id INT,
    session_id INT,
    transaction_timestamp TIMESTAMP,
    transaction_date_id INT,
    product_id INT,
    quantity INT,

    unit_cost DECIMAL(10,2),
    unit_price DECIMAL(10,2),

    line_cost DECIMAL(10,2),
    line_total DECIMAL(10,2),
    aov_category VARCHAR(20),

    FOREIGN KEY (product_id) REFERENCES dim_product(product_id),
    FOREIGN KEY (transaction_date_id) REFERENCES dim_date(date_id)
);
