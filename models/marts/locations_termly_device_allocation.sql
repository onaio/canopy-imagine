select
    le.partner_term_id,
    le.term_name,
    le.location_id,
    le.location,
    le.admin_3_name,
    le.country,
    le.partner,
    le.field_officer,
    le.lat,
    le.lon,
    coalesce(allocated_devices, 0) as allocated_devices
from {{ ref('location_enrollments') }} le
left join {{ref("int_devices_allocated")}} da on le.location_id = da.location_id and le.partner_term_id = da.term_id
group by 1,2,3,4,5,6,7,8,9,10,11