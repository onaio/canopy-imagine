select 
    wms.*,
    case when (wms."location" = '' or wms."location" = '---') then null else split_part(wms."location" ,' ', 1)::float end as latitude,
    case when (wms."location" = '' or wms."location" = '---') then null else split_part(wms."location" ,' ', 2)::float end as longitude,
	split_part(wms."location" ,' ', 4) as gps_accuracy,
    lw.term_week,
	lw.term_id,
	lw.term_name,
    p.name as iwon_partner_name,
    s.name as iwon_staff_name,
    l.name as iwon_school_name,
    l.country as iwon_country,
    l.admin_3_name,
    l.date_launched,
    l.lat as school_latitude,
    l.lon as school_longitude
from {{ref("stg_weekly_monitoring_survey")}} wms
left join {{ref("stg_partners")}} p on wms.iwon_partner_id  = p.id::text
left join {{ref("stg_staff")}}s on wms.iwon_user_id = s.id::text
left join {{ref("stg_locations")}} l on wms.iwon_school_id = l.id::text
left join {{ref("int_locations_weekly")}} lw on wms.iwon_school_id = lw.location_id::text and date_trunc('week', wms.observation_date::date)::date = lw.week and p.name = lw.partner
where observation_date != '---' and observation_date != ''