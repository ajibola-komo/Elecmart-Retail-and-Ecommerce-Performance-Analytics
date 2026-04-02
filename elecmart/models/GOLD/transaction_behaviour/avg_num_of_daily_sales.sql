-- A sale is considered a completed transaction

with daily_sales_count as (
    select transaction_timestamp::DATE as transaction_date, 
           count(distinct transaction_id) as daily_sales_count
    from {{ref('silver_fact_transaction')}}
    where transaction_status = 'Completed'
    group by transaction_timestamp::DATE
)
select avg(daily_sales_count) as average_daily_sales
from daily_sales_count