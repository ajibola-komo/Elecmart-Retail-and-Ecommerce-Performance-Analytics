select sale_id::INTEGER as sale_id,
transaction_id::INTEGER as transaction_id,
session_id::INTEGER as session_id,
transaction_timestamp::timestamp_ntz as transaction_timestamp,
transaction_date_id::INTEGER as transaction_date_id,
product_id::INTEGER as product_id,
quantity::INTEGER as quantity,
to_decimal(unit_cost) as unit_cost,
to_decimal(unit_price) as unit_price,
to_decimal(line_cost) as line_cost,
to_decimal(line_total) as line_total
from {{source('bronze','fact_sale')}}