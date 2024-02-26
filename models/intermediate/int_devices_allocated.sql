-- model with one row per each location per each term, with statistics on device
-- allocation

select
    location_id,
    location,
    term_id,
    term_name,
    term_start,
    term_end,
    admin_3_name,
    country,
    partner,
    field_officer,
    date_launched,
    count(device_id) as allocated_devices
from {{ ref("devices_per_term") }}
group by 1,2,3,4,5,6,7,8,9,10,11
