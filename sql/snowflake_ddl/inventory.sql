create OR REPLACE table inventory(
    inventory_id int primary key,
    product_id int,
    store_id int,
    snapshot_month date,
    starting_stock int,
    received_stock int,
    sold_units int,
    closing_stock int,
    backorder_flag boolean,
    shrinkage_loss int,
    foreign key(store_id) references dim_store(store_id),
    foreign key(product_id) references dim_product(product_id)
);