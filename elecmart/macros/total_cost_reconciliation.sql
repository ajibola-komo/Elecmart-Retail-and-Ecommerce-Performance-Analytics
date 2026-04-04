{% test total_cost_matches_transactions(model) %}

select *
from {{ model }}
where total_cost != (
    select coalesce(sum(line_cost), 0)
    from {{ ref('silver_fact_sale') }} fs inner join {{ ref('silver_fact_transaction') }} ft on fs.transaction_id = ft.transaction_id
    where ft.transaction_status = 'Completed'
)

{% endtest %}