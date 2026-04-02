CREATE TABLE DIM_SUBCATEGORY(
    subcategory_id INT PRIMARY KEY,
    subcategory_name VARCHAR,
    category_id INT,
    FOREIGN KEY(category_id) references DIM_CATEGORY(category_id)
);