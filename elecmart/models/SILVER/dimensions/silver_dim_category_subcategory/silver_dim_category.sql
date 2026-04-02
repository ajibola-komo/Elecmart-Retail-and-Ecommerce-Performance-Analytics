with deduplicate as (
    select category_id, initcap(category_name) as category_name, row_number() over(
        partition by category_id, category_name
        order by category_id
    ) as rn
    from {{source('bronze','dim_category')}}
) select category_id, category_name from deduplicate where rn = 1