-- Model with one row representing one device per term. Applies to devices allocated.

{{
    config(
        materialized='table'
    )
}}

select 
	da.id,
    d.device_id,
	da.tablet_serial_number as serial_number,
	da.term_id,
    t.name as term_name,
	t.start_date as term_start,
	t.end_date as term_end, 
	da.location_id,
    l."name" as location,
    l.admin_3_name as admin_3_name,
    l.country,
    p.name as partner,
    s.name as field_officer
from {{ref('stg_device_allocation')}} da 
left join  {{ref('stg_devices') }}  d on 
		d.serial_number = da.tablet_serial_number 
left join {{ref('stg_terms')}} t on da.term_id = t.id
left join {{ref('stg_locations')}} l on da.location_id = l.id 
left join {{ref('stg_staff')}} s on s.id = l.staff_id
left join {{ref('stg_partners') }} p on s.partner_id::int = p.id
