-- This script tests for invalid dates
select * from {{ref("silver_dim_date")}} where day < 1 or day > days_in_month