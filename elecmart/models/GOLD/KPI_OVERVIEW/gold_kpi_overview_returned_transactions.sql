select count(*) as total_returned_transactions,
coalesce(sum(transaction_total), 0) as total_returned_value,
coalesce(sum(transaction_cost), 0) as total_cost_for_returned_transactions
from {{ ref('gold_fact_returns') }}