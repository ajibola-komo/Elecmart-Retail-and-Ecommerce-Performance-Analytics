select ft.customer_id,
case when ft.customer_id is null then 'Guest' else 'Registered' end as customer_type,
loyalty_status,
customer_persona,
count(distinct ft.transaction_id) as total_orders,
sum(ft.transaction_total) as total_spent,
avg(ft.transaction_total) as average_order_value,
min(ft.transaction_timestamp::DATE) as first_order_date,
max(ft.transaction_timestamp::DATE) as last_order_date
from {{ref('silver_fact_transaction')}} ft inner join {{ref('silver_dim_customer')}} dc on ft.customer_id = dc.customer_id
group by ft.customer_id, customer_type, loyalty_status, customer_persona
order by total_spent desc, total_orders desc