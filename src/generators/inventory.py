import numpy as np
from src.config.paths import (INVENTORY_DDL_PATH, INVENTORY_CSV_PATH, INVENTORY_PARQUET_PATH)
from src.config.constants import(SHRINKAGE_RATE)

def generate_inventories(conn):
    
    create_db = INVENTORY_DDL_PATH.read_text()

    conn.execute(create_db)

    query = '''WITH MONTH_SPINE as (SELECT DISTINCT date_trunc('month', transaction_timestamp) as month_start from fact_transaction),
    SKU_GRID as (SELECT p.product_id, s.store_id, m.month_start as snapshot_month from dim_product as p CROSS JOIN dim_store as s CROSS JOIN MONTH_SPINE as m),
    MONTHLY_SALES as (SELECT s.product_id, t.store_id, date_trunc('month', t.transaction_timestamp) as snapshot_month, sum(s.quantity) as sold_units from fact_transaction as t inner join fact_sale as s on t.transaction_id = s.transaction_id
    where t.transaction_status = 'Completed' group by s.product_id,t.store_id,snapshot_month)
    SELECT g.product_id, g.store_id, g.snapshot_month, coalesce(s.sold_units,0) as sold_units from sku_grid as g left join monthly_sales s on 
    s.product_id = g.product_id and s.store_id = g.store_id and s.snapshot_month = g.snapshot_month
    order by g.product_id, g.store_id, g.snapshot_month'''

    sku_grid = conn.execute(query).df()

    stock_skeleton = sku_grid[['product_id','store_id','snapshot_month']].copy()
    stock_skeleton['sold_units'] = sku_grid['sold_units']
    stock_skeleton['starting_stock'] = 0
    stock_skeleton['received_stock'] = 0
    stock_skeleton['closing_stock'] = 0
    stock_skeleton['backorder_flag'] = False
    stock_skeleton['shrinkage_loss'] = 0
    stock_skeleton['inventory_id'] = np.arange(8935, 8935 + len(stock_skeleton))

    first_month = stock_skeleton['snapshot_month'].min()

    mask = stock_skeleton['snapshot_month'] == first_month
    first_month_sales = stock_skeleton.loc[mask, 'sold_units']
    stock_skeleton.loc[mask, 'starting_stock'] = np.maximum(
    np.random.randint(25, 50, size=mask.sum()),
    (first_month_sales * 1.5).astype(int)
)
    stock_skeleton['received_stock'] = np.maximum(
    np.random.randint(0, 10, size=len(stock_skeleton)),
    (sku_grid['sold_units'] * 1.2).astype(int)
)

    stock_skeleton = stock_skeleton.sort_values(by=['product_id', 'store_id','snapshot_month'])

    has_shrinkage = np.random.rand(len(stock_skeleton)) <= SHRINKAGE_RATE

    stock_skeleton.loc[has_shrinkage,'shrinkage_loss'] = np.random.randint(0,2, size=has_shrinkage.sum())
    

    conn.register("stock_data",stock_skeleton)

    query = '''
WITH inventory_flow AS (
    SELECT
        inventory_id,
        product_id,
        store_id,
        snapshot_month,
        received_stock,
        sold_units,
        shrinkage_loss,

        SUM(received_stock - sold_units - shrinkage_loss)
        OVER (
            PARTITION BY product_id, store_id
            ORDER BY snapshot_month
        ) AS cumulative_net,

        FIRST_VALUE(starting_stock)
        OVER (
            PARTITION BY product_id, store_id
            ORDER BY snapshot_month
        ) AS initial_stock

    FROM stock_data
),

final_inventory AS (
    SELECT
        inventory_id,
        product_id,
        store_id,
        snapshot_month,
        initial_stock,

        initial_stock + cumulative_net AS raw_closing_stock,

        GREATEST(initial_stock + cumulative_net, 0) AS closing_stock,

        received_stock,
        sold_units,
        shrinkage_loss
    FROM inventory_flow
)

INSERT INTO INVENTORY
SELECT
    inventory_id,
    product_id,
    store_id,
    snapshot_month,

    -- ✅ FIXED
    LAG(closing_stock, 1, initial_stock)
        OVER (PARTITION BY product_id, store_id ORDER BY snapshot_month),

    received_stock,
    sold_units,
    closing_stock,

    raw_closing_stock < 0 AS backorder_flag,

    shrinkage_loss
FROM final_inventory
    '''

    conn.execute(query)

    conn.execute(f'''
                    COPY INVENTORY TO '{INVENTORY_PARQUET_PATH}' (FORMAT PARQUET)
''')
