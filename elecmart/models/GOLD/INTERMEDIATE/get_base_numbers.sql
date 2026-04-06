SELECT sum(transaction_total) as total_sales_revenue, sum(transaction_cost) as cogs,
sum(quantity) as total_units_sold, count(distinct t.transaction_id) as total_transactions
 from {{ ref('gold_fact_completed_transaction') }} t inner join {{ ref('gold_fact_completed_sale') }} s on
 t.transaction_id = s.transaction_id