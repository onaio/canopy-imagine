-- model representing one row for each location in each term week, but including summary metrics like the number of sessions recorded and the reporting devices

with weekly_sessions as (
select 
    date_trunc('week',start_time) as week, 
    location_id,
    is_last_week,
	COUNT(DISTINCT device_id) as reporting_devices,
	COUNT(DISTINCT id) as session_records,
	SUM(coalesce(duration,0))/60 as actual_mins
    from {{ref('sessions')}}
    group by 1,2,3
)

select 
	tw.week,
	tw.week_number as term_week,
	tw.term_id,
	ce.term_name,
    ce.partner,
	ce.location_id,
	ce.location,
	ce.country,
	ce.field_officer,
	se.is_last_week,
	ce.children,
    da.allocated_devices,
    coalesce(se.reporting_devices,0) as reporting_devices,
	coalesce(se.session_records,0) as session_records,
	coalesce(se.actual_mins,0) as actual_mins,
	(cm.value*5 * ce.children) as expected_mins   --expected minutes is number of minutes per day * 5 days per week

from  {{ref('stg_term_weeks')}} tw  
left join {{ref('int_locations_per_term')}}  ce on ce.term_id = tw.term_id 
left join {{ ref('int_devices_allocated') }} da on tw.term_id = da.term_id and ce.location_id = da.location_id 
left join weekly_sessions se on se.week = tw.week and ce.location_id = se.location_id
left join {{ref('stg_country_metrics')}} cm on cm.country = ce.country and cm.partner = ce.partner
where tw.week <= current_date 