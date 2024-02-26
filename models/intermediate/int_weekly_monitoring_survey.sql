select 
    wms.*,
    observaton_date::date as observation_date,
    lw.term_week,
	lw.term_id,
	lw.term_name,
    p.name as iwon_partner_name,
    s.name as iwon_staff_name,
    l.name as iwon_school_name,
    l.country as iwon_country,
    l.date_launched
from {{source ('commcare', 'weekly_monitoring_survey')}} wms
left join {{ref("stg_partners")}} p on wms.iwon_partner_id  = p.id::text
left join {{ref("stg_staff")}}s on wms.iwon_user_id = s.id::text
left join {{ref("stg_locations")}} l on wms.iwon_school_id = l.id::text
left join {{ref("int_locations_weekly")}} lw on wms.iwon_school_id = lw.location_id::text and date_trunc('week', wms.observaton_date::date)::date = lw.week and p.name = lw.partner
where observaton_date != '---' and observaton_date != ''