select
    *
from {{ ref('silver_fact_transaction') }}
where transaction_status = 'Returned'