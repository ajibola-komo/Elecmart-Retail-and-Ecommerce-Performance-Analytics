with product_sales as (

    select
        dp.product_id,
        dp.product_name,
        sum(ft.transaction_subtotal) as revenue,
        sum(ft.transaction_subtotal) - sum(fs.line_cost) as profit,
        ((sum(ft.transaction_subtotal) - sum(fs.line_cost)) / sum(ft.transaction_subtotal)) * 100 as margin,
        sum(iv.starting_stock) as stock,
        sum(fs.quantity) / (datediff(day, min(fs.transaction_timestamp::date), max(fs.transaction_timestamp::date)) + 1) as avg_daily_sales
    from {{ ref('silver_dim_product') }} dp
    join {{ ref('silver_fact_sale') }} fs
        on dp.product_id = fs.product_id
    join {{ ref('silver_inventory') }} iv
        on iv.product_id = dp.product_id
    join {{ ref('silver_fact_transaction') }} ft
        on fs.transaction_id = ft.transaction_id
    group by dp.product_id, dp.product_name

)

select
    *,
    stock / nullif(avg_daily_sales,0) as days_until_stockout
from product_sales