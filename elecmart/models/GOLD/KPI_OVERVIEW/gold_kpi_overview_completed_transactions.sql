with get_revenue as (
select transaction_id, sum(transaction_total) as total_sales from {{ref('silver_fact_transaction')}} where transaction_status = 'Completed'
group by transaction_id
), get_cost as (
select transaction_id, sum(line_cost) as total_cost from {{ref('silver_fact_sale')}} group by transaction_id
), join_transactions as (
select gt.transaction_id, gt.total_sales, gc.total_cost from get_revenue gt inner join get_cost gc on gt.transaction_id = gc.transaction_id
), final as (
select sum(total_sales) as total_sales, sum(total_cost) as total_cost, sum(total_sales - total_cost) as gross_profit from join_transactions
)
select total_sales, total_cost, gross_profit, round((gross_profit / total_sales) * 100, 2) as profit_margin_percentage from final