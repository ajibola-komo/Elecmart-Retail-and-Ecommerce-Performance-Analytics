{% test completed_aov(model) %}

select *
from {{ model }}
where aov != (
    select round(sum(case when transaction_status = 'Completed' then net_line_revenue else 0 end) 
    / nullif(count(distinct transaction_id), 0), 2)
    from {{ ref('gold_fact_sale') }} 
)

{% endtest %}