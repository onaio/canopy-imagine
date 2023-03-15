select 
id::int, 
lat::float,
lon::float,
rtrim(name) as name,
rtrim(type) as type,
RTRIM(country) as country,
staff_id::int,
rtrim(admin_3_name) as admin_3_name
from {{source('airbyte', 'locations')}}