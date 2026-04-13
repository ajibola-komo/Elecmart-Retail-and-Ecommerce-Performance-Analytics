select *, to_char(date, 'YYYYMM') as month_id,
    year || '-Q' || quarter as year_quarter
 from {{ref('silver_dim_date')}}