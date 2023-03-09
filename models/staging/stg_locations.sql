select 
id::int, 
lat::float,
lon::float,
name,
type,
RTRIM(country) as country,
staff_id::int,
admin_3_name
from {{source('airbyte', 'locations')}}