select dp.product_id, dp.product_name,
sum(ft.transaction_subtotal) as total_revenue,
sum(fs.line_cost) as total_cost,
sum(ft.transaction_subtotal) - sum(fs.line_cost) as total_profit,
total_profit / sum(ft.transaction_subtotal) * 100 as profit_margin_percentage
from {{ref("silver_dim_product")}} dp inner join {{ref("silver_fact_sale")}} fs on dp.product_id = fs.product_id
inner join {{ref('silver_fact_transaction')}} ft on fs.transaction_id = ft.transaction_id
group by dp.product_id, dp.product_name
order by total_profit desc