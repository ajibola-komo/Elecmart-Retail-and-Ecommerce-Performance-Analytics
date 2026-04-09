{% test gross_profit_reconciliation(model) %}
select *
from {{ model }}
where gross_profit != (
    select round(coalesce(sum(case when transaction_status = 'Completed' then net_line_revenue else 0 end), 0) - 
    coalesce(sum(case when transaction_status = 'Completed' then line_cost else 0 end), 0), 2)
    from {{ ref('gold_fact_sale') }}
)

{% endtest %}