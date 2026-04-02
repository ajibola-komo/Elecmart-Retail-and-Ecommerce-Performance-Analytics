with get_summary as (
    SELECT ft.customer_id, case when ft.customer_id is null then 'Anonymous' else dc.loyalty_status end as
    loyalty_status, sum(transaction_total) AS total_revenue
    FROM {{ ref('silver_fact_transaction') }} ft
    left JOIN {{ ref('silver_dim_customer') }} dc
        ON ft.customer_id = dc.customer_id
    group by 1,2
) select loyalty_status, sum(total_revenue) as revenue_by_segment
from get_summary
group by loyalty_status