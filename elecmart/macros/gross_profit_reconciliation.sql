{% test gross_profit_reconciliation(model) %}
select *
from {{ model }}
where gross_profit != (
    select round(coalesce(sum(total_completed_sales), 0) - coalesce(sum(total_completed_cost), 0), 2)
    from {{ ref('get_rev_and_cost') }}
)

{% endtest %}