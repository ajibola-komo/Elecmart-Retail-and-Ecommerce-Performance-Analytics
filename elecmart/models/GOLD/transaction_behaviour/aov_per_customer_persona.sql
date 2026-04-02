select customer_persona, 
       AVG(transaction_total) AS average_order_value
       from {{ref('silver_fact_transaction')}} ft
inner join {{ref('silver_dim_customer')}} dc on ft.customer_id = dc.customer_id
where transaction_status = 'Completed'
group by customer_persona
order by average_order_value desc
