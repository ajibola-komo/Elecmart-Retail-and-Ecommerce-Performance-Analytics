CREATE OR REPLACE TABLE DIM_PRODUCT(
    product_id INT PRIMARY KEY,
    product_name VARCHAR,
    category_id INT,
    subcategory_id INT,
    brand_id INT,
    unit_cost DECIMAL(10,2),
    unit_price DECIMAL(10,2),
    warranty_years INT,
    product_segment VARCHAR,
    FOREIGN KEY(category_id) references DIM_CATEGORY(category_id),
    FOREIGN KEY(subcategory_id) references DIM_SUBCATEGORY(subcategory_id),
    FOREIGN KEY(brand_id) references DIM_BRAND(brand_id)
);