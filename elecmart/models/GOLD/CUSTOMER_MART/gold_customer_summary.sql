select dc.customer_id,
case when fs.customer_id is null then 'Guest' else 'Registered' end as customer_type,
loyalty_status,
customer_persona,
count(distinct fs.transaction_id) as total_orders,
sum(case when fs.transaction_status = 'Completed' then fs.net_line_revenue else 0 end) as total_spent,
avg(case when fs.transaction_status = 'Completed' then fs.net_line_revenue else null end) as average_order_value,
sum(case when fs.transaction_status = 'Completed' then fs.quantity else 0 end) as total_units_purchased,
sum(case when fs.transaction_status = 'Returned' then fs.net_line_revenue else 0 end) as total_returned,
sum(case when fs.transaction_status = 'Returned' then fs.quantity else 0 end) as total_units_returned,
min(fs.transaction_timestamp::DATE) as first_order_date,
max(fs.transaction_timestamp::DATE) as last_order_date
from {{ref('gold_fact_sale')}} fs inner join {{ref('gold_dim_customer')}} dc on fs.customer_id = dc.customer_id
group by dc.customer_id, customer_type, loyalty_status, customer_persona
order by total_spent desc, total_orders desc