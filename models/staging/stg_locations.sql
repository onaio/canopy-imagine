select 
id::int, 
lat::float,
lon::float,
name,
type,
country,
staff_id::int,
admin_3_name
from {{source('airbyte', 'locations')}}