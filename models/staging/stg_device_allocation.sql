select 
id::int,
term_id::int,
school_id::int,
rtrim(tablet_serial_number) as tablet_serial_number,
rtrim(comment) as comment 
from {{source('airbyte', 'device_allocation')}}