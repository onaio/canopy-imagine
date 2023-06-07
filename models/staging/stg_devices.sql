select 
id::int,
rtrim(country) as country,
rtrim(device_id) as device_id,
rtrim(serial_number) as serial_number 
from {{source('airbyte', 'devices')}}