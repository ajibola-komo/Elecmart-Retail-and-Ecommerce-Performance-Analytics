with source_data as (
    select brand_id, brand_name, row_number() over(
        partition by brand_name
        order by brand_id
    ) as rn from {{source('bronze','dim_brand')}}
), deduplicated as (
    select * from source_data where rn = 1
)
select brand_id, brand_name from deduplicated order by brand_id