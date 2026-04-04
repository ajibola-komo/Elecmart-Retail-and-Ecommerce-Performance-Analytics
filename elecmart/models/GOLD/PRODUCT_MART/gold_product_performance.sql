select product_id, ft.transaction_timestamp::DATE as transaction_date,
sum(ft.transaction_total) as total_sales,
sum(fs.line_cost) as total_cost,
sum(ft.transaction_total) - sum(fs.line_cost) as gross_profit,
round((sum(ft.transaction_total) - sum(fs.line_cost)) / sum(ft.transaction_total) * 100, 2) as profit_margin_percentage,
sum(fs.quantity) as total_units_sold,
from {{ref('silver_fact_sale')}} fs inner join {{ref('silver_fact_transaction')}} ft on fs.transaction_id = ft.transaction_id
group by product_id, transaction_date
order by transaction_date, product_id

