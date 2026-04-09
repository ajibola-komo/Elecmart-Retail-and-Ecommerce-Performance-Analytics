{% test total_sales_matches_transactions(model) %}

select *
from {{ model }}
where total_revenue != (
    select coalesce(sum(net_line_revenue), 0)
    from {{ ref('gold_fact_sale') }} where transaction_status = 'Completed'
)

{% endtest %}