select sales_channel, 
        count(distinct transaction_id) as total_transactions,
       round(AVG(transaction_total), 2) AS average_order_value
       from {{ref('silver_fact_transaction')}} ft
       where transaction_status = 'Completed'
group by sales_channel