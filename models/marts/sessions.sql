select 
row_number() over( order by ts.start_time) as id,
ts.device_id,
user_id, 
start_time::timestamp ,
end_time::timestamp ,
duration ,
literacy_level ,
numeracy_level ,
s.id as school_id,
s."name" as school,
s.admin_3_name as location,
s.country,
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
left join {{ref('stg_locations')}} s on dt.school_id = s.id 
left join {{ref('stg_term_weeks')}} tw on 
	dt.term_id = tw.term_id and
	date_trunc('week',(ts.start_time::date)) = tw.week  
order by device_id ,session_id 
