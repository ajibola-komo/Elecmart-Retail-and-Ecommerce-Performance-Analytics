WITH monthly_sales AS (
SELECT
    dp.product_id,
    date_trunc('month', transaction_timestamp) AS sales_month,
    SUM(line_cost) AS total_cogs,
    SUM(quantity) AS sold_units

FROM {{ref('silver_dim_product')}} dp 
left join {{ ref('silver_fact_sale') }} fs
    ON dp.product_id = fs.product_id

GROUP BY
    dp.product_id,
    date_trunc('month', transaction_timestamp)
)

SELECT
    ms.product_id,
    iv.snapshot_month,

    SUM(iv.starting_stock) AS starting_stock,
    SUM(iv.closing_stock) AS closing_stock,

    (SUM(iv.starting_stock) + SUM(iv.closing_stock)) / 2 AS average_inventory,

    ms.total_cogs /
        NULLIF((SUM(iv.starting_stock) + SUM(iv.closing_stock)) / 2, 0)
        AS inventory_turnover,

    (ms.sold_units /
        NULLIF(SUM(iv.received_stock),0)) * 100
        AS sell_through_rate,

    (((SUM(iv.starting_stock) + SUM(iv.closing_stock)) / 2) /
        NULLIF(ms.total_cogs,0)) * dd.days_in_month
        AS days_of_inventory_on_hand

FROM {{ ref('silver_inventory') }} iv

JOIN {{ ref('silver_dim_date') }} dd
    ON iv.snapshot_month_id = dd.date_id

LEFT JOIN monthly_sales ms
    ON iv.product_id = ms.product_id
    AND iv.snapshot_month = ms.sales_month

GROUP BY
    ms.product_id,
    iv.snapshot_month,
    ms.total_cogs,
    ms.sold_units,
    dd.days_in_month

ORDER BY
    ms.product_id,
    iv.snapshot_month