select 
id::int,
country,
device_id,
serial_number 
from {{source('airbyte', 'devices')}}