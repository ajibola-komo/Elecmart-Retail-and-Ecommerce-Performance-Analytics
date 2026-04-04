{% test profit_margin_reconciliation(model) %}
select *
from {{ model }}
where profit_margin_percentage != (
    select 
round((coalesce(sum(total_completed_sales), 0) - coalesce(sum(total_completed_cost), 0)) / nullif(coalesce(sum(total_completed_sales), 0), 0) * 100, 2)
    from {{ ref('get_rev_and_cost') }}
)

{% endtest %}