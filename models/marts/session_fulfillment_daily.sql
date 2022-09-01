---- reporting.daily_fulfillment
-- needs testing with updated dataset due to missing records
with child_enrollment_location as (
select
	term_id,
	location_id,
	SUM(number::int) as children
from {{ref('stg_location_enrollments')}}
group by 1,2
), child_enrollment_country as (
select
	term_id,
	SUM(number::int) as children_country
from {{ref('stg_location_enrollments')}}
group by 1
), actual_vs_expected as (
select 
	td.day,
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
	cec.children_country,
	SUM(se.duration/60) as actual_mins,
	AVG(cm.value::int) as expected_mins
from {{ref('stg_term_days')}} td
left join {{ref('sessions')}} se on date_trunc('day',(se.start_time::date)) = td.day and td.term_id = se.term_id 
left join {{ref('stg_term_weeks')}} tw on date_trunc('week',(td.day::date)) = tw.week  and tw.term_id = se.term_id 
left join {{ref('stg_country_metrics')}} cm on cm.country = se.country
left join child_enrollment_location ce on ce.term_id = se.term_id and ce.location_id = se.location_id 
left join child_enrollment_country cec on cec.term_id = se.term_id
where se.location_id is not null
group by 1,2,3,4,5,6,7,8,9,10,11,12
), max_term_week as (
select
	term_id,
	MAX(term_week) as week_count
from actual_vs_expected
group by 1
)

select 
	main.day,
	main.week,
	main.term_week,
	main.term_id,
	main.term_name, 
	main.location_id,
	main.location,
	main.country,
	main.field_officer,
	main.is_last_week,
	main.children,
	main.children_country,
	mtw.week_count,
	main.actual_mins,
	main.expected_mins,
	case when main.actual_mins >= main.expected_mins*main.children then 1 else 0 end as fullfillment 
from actual_vs_expected main
left join max_term_week mtw on mtw.term_id = main.term_id
