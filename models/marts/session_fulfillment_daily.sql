---- reporting.daily_fulfillment
-- needs testing with updated dataset due to missing records
with child_enrollment as (
select
	term_id,
	location_id,
	SUM(number::int) as children
from {{source('airbyte','location_enrollments')}}
group by 1,2
), 
actual_vs_expected as (
select 
	td.day,
	tw.week,
	tw.week_number as term_week,
	se.location_id,
	se.location,
	se.country,
	SUM(se.duration) as actual_mins,
	AVG((cm.value::int)*ce.children) as expected_mins
from {{ref('stg_term_days')}} td
left join {{ref('stg_term_weeks')}} tw on date_trunc('week',(td.day::date)) = tw.week 
left join {{ref('sessions')}} se on date_trunc('day',(se.start_time::date)) = td.day and td.term_id = se.term_id 
left join {{source('airbyte','country_metrics')}} cm on cm.country = se.country
left join child_enrollment ce on ce.term_id = se.term_id and ce.location_id = se.location_id 
group by 1,2,3,4,5,6
) 

select 
	day,
	week,
	term_week,
	location_id,
	location,
	country,
	actual_mins,
	expected_mins,
	case when actual_mins >= expected_mins then 1 else 0 end as fullfillment
from actual_vs_expected
where location_id is not null
