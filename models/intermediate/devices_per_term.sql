select 
	da.id,
	da.term_id,
	t.start_date::date as term_start,
	t.end_date::date as term_end, 
	t.name as term_name,
	da.school_id ,
	da.tablet_serial_number
from {{ref('stg_device_allocation')}} da 
left join {{ref('stg_terms')}} t on da.term_id = t.id