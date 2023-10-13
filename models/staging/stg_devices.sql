select 
rtrim(device_id) as device_id,
rtrim(serial_number) as serial_number 
from {{source('directus', 'devices')}}