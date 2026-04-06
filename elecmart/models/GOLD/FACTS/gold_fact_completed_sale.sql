with get_completed_transactions as (
    select transaction_id from {{ ref('gold_fact_completed_transaction') }}
), get_completed_sales as (
    select gct.transaction_id, sale_id, session_id, transaction_timestamp,
    transaction_date_id, product_id, quantity, unit_cost,unit_price, line_cost, line_total from get_completed_transactions gct left join {{ ref('gold_fact_sale') }} gs on 
    gct.transaction_id = gs.transaction_id
) select * from get_completed_sales