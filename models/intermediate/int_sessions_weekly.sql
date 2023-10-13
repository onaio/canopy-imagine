with child_enrollment as (
select
	term_id,
	location_id,
	SUM(number::int) as children
from {{ref('stg_location_enrollments')}}
group by 1,2
) 

select 
	tw.week,
	tw.week_number as term_week,
	tw.term_id,
	tw.term_name,
	se.location_id,
	se.location,
	se.country,
	se.field_officer,
	se.is_last_week,
	ce.children,
    cc.cumulative_sessions,
	COUNT(DISTINCT se.device_id) as reporting_devices,
	COUNT(DISTINCT se.id) as session_records,
	SUM(se.duration/60) as actual_mins,
	AVG(cm.value::int) as expected_mins

from {{ref('stg_term_days')}} td
left join {{ref('sessions')}} se on date_trunc('day',(se.start_time::date)) = td.day and td.term_id = se.term_id 
left join {{ref('stg_term_weeks')}} tw on date_trunc('week',(td.day::date)) = tw.week  and tw.term_id = se.term_id 
left join {{ref('stg_country_metrics')}} cm on cm.country = se.country and cm.partner = se.partner
left join child_enrollment ce on ce.term_id = se.term_id and ce.location_id = se.location_id
left join {{ref('sessions_cumulative_count')}} cc on cc.week = tw.week and cc.location_id = se.location_id and cc.term_id = tw.term_id 
where tw.week <= current_date
group by 1,2,3,4,5,6,7,8,9,10,11