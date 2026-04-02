select dp.product_id, dp.product_name, category_name, sum(quantity) as units_sold,
sum(transaction_subtotal) as total_revenue
from {{ref("silver_dim_product")}} dp inner join {{ref("silver_fact_sale")}} fs on dp.product_id = fs.product_id
inner join {{ref('silver_fact_transaction')}} ft on fs.transaction_id = ft.transaction_id
inner join {{ref('silver_dim_category')}} dc on dp.category_id = dc.category_id
group by dp.product_id, dp.product_name, category_name
order by dp.product_id