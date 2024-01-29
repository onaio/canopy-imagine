select 
    wms.*,
    observaton_date::date as observation_date,
    p.name as partner_name,
    s.name as staff_name
from {{source ('commcare', 'weekly_monitoring_survey')}} wms
left join {{ref("stg_partners")}} p on wms.iwon_partner_id  = p.id::text
left join {{ref("stg_staff")}}s on wms.iwon_user_id = s.id::text
where observaton_date != '---'