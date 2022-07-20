select 
row_number() over( order by ts.start_time) as id,
ts.device_id,
user_id, 
start_time::timestamp ,
end_time::timestamp ,
duration ,
literacy_level ,
numeracy_level ,
l.id as location_id,
l."name" as location,
l.admin_3_name as admin_3_name,
l.country,
date_trunc('week', start_time::timestamp)::date as week, 
dt.term_id ,
dt.term_name,
tw.week_number as term_week
from {{ref('stg_sessions')}} ts 
left join {{ref('stg_devices') }}  d on ts.device_id = d.serial_number
left join {{ref('devices_per_term')}} dt on 
	d.serial_number = dt.tablet_serial_number and 
	ts.start_time::date > dt.term_start and 
	ts.start_time::date < dt.term_end
left join {{ref('stg_locations')}} l on dt.school_id = l.id 
left join {{ref('stg_term_weeks')}} tw on 
	dt.term_id = tw.term_id and
	date_trunc('week',(ts.start_time::date)) = tw.week  
order by device_id ,session_id 
