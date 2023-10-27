{{
    config(
        materialized='table'
    )
}}

select 
    dt.id,
    d.device_id,
    d.serial_number,
    l.id as location_id,
    l."name" as location,
    l.admin_3_name as admin_3_name,
    l.country,
    p.name as partner,
    s.name as field_officer,
    dt.term_id ,
    dt.term_name,
    tw.week,
    tw.week_number
from 
{{ref('devices_per_term')}} dt  
left join  {{ref('stg_devices') }}  d on 
		d.serial_number = dt.tablet_serial_number 
left join {{ref('stg_locations')}} l on dt.location_id = l.id 
left join {{ref('stg_staff')}} s on s.id = l.staff_id
left join {{ ref('stg_partners') }} p on s.partner_id::int = p.id	
left join {{ ref('stg_term_weeks') }} tw on dt.term_id = tw.term_id 