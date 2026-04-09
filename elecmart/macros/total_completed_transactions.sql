{% test total_completed_transactions(model) %}

select *
from {{ model }}
where total_transactions != (
    select coalesce(count(distinct transaction_id), 0)
    from {{ ref('gold_fact_sale') }} where transaction_status = 'Completed'
)

{% endtest %}