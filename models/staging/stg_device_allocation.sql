select 
id::int,
term_id::int,
location_id::int,
rtrim(serial_number) as tablet_serial_number,
rtrim(comment) as comment 
from {{source('directus', 'device_allocation')}}