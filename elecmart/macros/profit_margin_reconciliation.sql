{% test profit_margin_reconciliation(model) %}
select *
from {{ model }}
where profit_margin != (
    select 
round(
    (coalesce(sum(net_line_revenue),0) - coalesce(sum(line_cost), 0))
/ nullif(coalesce(sum(net_line_revenue), 0), 0) * 100, 2)
    from {{ ref('gold_fact_sale') }} where transaction_status = 'Completed'
)
{% endtest %}