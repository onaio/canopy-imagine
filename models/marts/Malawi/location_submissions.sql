select
    l.id as location_id,
    l.country,
    p.name as partner,
    l.admin_3_name,
    l.name as location,
    l.lat,
    l.lon,
    coalesce(count(wms.*), 0) as total_submissions
from {{ref("locations")}} l
left join {{ref("stg_staff")}} s on l.staff_id = s.id
left join{{ref("stg_partners")}} p on s.partner_id = p.id
left join {{ref("weekly_monitoring_survey")}} wms on l.id = wms.location_id
where l.country = 'Malawi'
group by 1,2,3,4,5,6,7