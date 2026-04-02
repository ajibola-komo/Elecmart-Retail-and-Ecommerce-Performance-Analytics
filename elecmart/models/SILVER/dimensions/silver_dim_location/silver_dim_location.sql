select location_id, initcap(country) as country, initcap(state_province) as state_province, initcap(city) as city, initcap(location_type) as location_type
from {{source('bronze','dim_location')}}
