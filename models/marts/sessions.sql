 WITH main_sessions AS (
	select 
		row_number() over( order by ts.start_time) as id,
		ts.country as tablet_country,
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
		s.name as field_officer,
		date_trunc('week', start_time::timestamp)::date as week,
		dt.term_id ,
		dt.term_name,
		tw.week_number as term_week
	from {{ref('stg_sessions_unique')}} ts 
	left join {{ref('stg_devices') }}  d on ts.device_id = d.serial_number and ts.country = d.country
	left join {{ref('devices_per_term')}} dt on 
		d.serial_number = dt.tablet_serial_number and 
		ts.start_time::date > dt.term_start and 
		ts.start_time::date < dt.term_end
	left join {{ref('stg_locations')}} l on dt.school_id = l.id and ts.country = l.country
	left join {{ref('stg_term_weeks')}} tw on 
		dt.term_id = tw.term_id and
		date_trunc('week',(ts.start_time::date)) = tw.week  
	left join {{ref('stg_staff')}} s on s.id = l.staff_id
	order by device_id ,session_id
), recent_data AS (
	select
		location_id,
		field_officer,
		MAX(date_trunc('week', start_time::timestamp)::date) as most_recent_date
	from main_sessions 
	group by 1,2
)

select
	ms.id,
	ms.device_id,
	ms.tablet_country,
	ms.user_id, 
	ms.start_time::timestamp,
	ms.end_time::timestamp,
	ms.duration,
	ms.literacy_level,
	ms.numeracy_level,
	ms.location_id,
	ms.location,
	ms.admin_3_name,
	ms.country,
	ms.field_officer,
	ms.week,
	ms.term_id,
	ms.term_name,
	ms.term_week,
	case when date_trunc('week', start_time::timestamp)::date = rd.most_recent_date then 'Yes' else 'No' end as is_last_week,
	case when rt.latest_term = true and ms.term_id = rt.id then 'Yes' else 'No' end as is_latest_term
from main_sessions ms
left join recent_data rd on rd.location_id = ms.location_id and rd.field_officer = ms.field_officer
left join {{ref('stg_terms')}} rt on rt.country = ms.country and rt.id = ms.term_id
order by ms.id


