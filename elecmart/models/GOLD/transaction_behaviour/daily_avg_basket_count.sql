-- average basket size per ay
select transaction_timestamp::DATE as transaction_date, 
       avg(items_count) as average_items_sold
from {{ref('silver_fact_transaction')}}
where transaction_status = 'Completed'
group by transaction_timestamp::DATE
order by transaction_date, average_items_sold desc