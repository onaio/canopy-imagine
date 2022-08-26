select 
id::int,
term_id::int,
school_id::int,
tablet_serial_number,
comment 
from {{source('airbyte', 'device_allocation')}}