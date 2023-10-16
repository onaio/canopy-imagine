select 
	da.id,
	da.term_id,
	t.start_date as term_start,
	t.end_date as term_end, 
	t.name as term_name,
	da.location_id,
	da.tablet_serial_number
from {{ref('stg_device_allocation')}} da 
left join {{ref('stg_terms')}} t on da.term_id = t.id