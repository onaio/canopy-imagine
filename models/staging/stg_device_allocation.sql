select 
NULL::int as id,     --2023.10.26 AP. Temp fix to enable group by and avoid duplicates. Original : id::int as id,
term_id::int,
location_id::int,
rtrim(serial_number) as tablet_serial_number,
rtrim(comment) as comment 
from {{source('directus', 'device_allocation')}}
group by 1,2,3,4,5 --2023.10.26 AP. line to remove once IWON data has been cleaned