
    {{
      config(
        alias = 'kpi_monthly_sales_and_profitability'
        )
    }}

with monthly_sales as (
    select date_trunc('Month', ft.transaction_timestamp) as sales_month,
    sum(ft.transaction_total) as total_sales,
    sum(fs.line_cost) as total_cost,
    count(distinct ft.transaction_id) as total_orders,
    avg(ft.transaction_total) as aov
    from {{ref('silver_fact_transaction')}} ft inner join {{ref('silver_fact_sale')}} fs on ft.transaction_id = fs.transaction_id
    where transaction_status = 'Completed'
    group by 1
), monthly_profit as (
    select sales_month, total_sales, total_cost, total_orders, aov,
    total_sales - total_cost as total_profit
    from monthly_sales
),
get_prev_sales_and_profit as (
select *, lag(total_sales) over(
    order by sales_month
) as prev_month_sales, lag(total_profit) over(
    order by sales_month
) as prev_month_profit
from monthly_profit
), final as (select sales_month, total_sales, total_orders, aov, total_profit,
round((total_sales - prev_month_sales)/nullif(prev_month_sales, 0), 2) * 100 as mom_sales_growth_pct,
round((total_profit - prev_month_profit)/nullif(prev_month_profit, 0), 2) * 100 as mom_profit_growth_pct
from get_prev_sales_and_profit
) select * from final order by sales_month
