with get_revenue as (
select transaction_id, sum(transaction_total) as total_sales from {{ref('silver_fact_transaction')}} where transaction_status = 'Completed'
group by transaction_id
), get_cost as ( 
select transaction_id, sum(line_cost) as total_cost from {{ref('silver_fact_sale')}} group by transaction_id
)select gr.transaction_id, total_sales as total_completed_sales, total_cost as total_completed_cost 
from get_revenue gr inner join get_cost gc on gr.transaction_id = gc.transaction_id