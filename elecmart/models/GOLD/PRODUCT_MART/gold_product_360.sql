select fs.product_id, product_name, category_name, brand_name, 
sum(ft.transaction_total) as total_sales,
sum(fs.line_cost) as total_cost,
sum(ft.transaction_total) - sum(fs.line_cost) as gross_profit
from {{ref('silver_fact_sale')}} fs inner join {{ref('silver_fact_transaction')}} ft on fs.transaction_id = ft.transaction_id
inner join {{ref('silver_dim_product')}} dp on fs.product_id = dp.product_id
inner join {{ref('silver_dim_category')}} dc on dp.category_id = dc.category_id
inner join {{ref('silver_dim_brand')}} db on dp.brand_id = db.brand_id
group by fs.product_id, product_name, category_name, brand_name