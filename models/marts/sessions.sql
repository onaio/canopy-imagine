 WITH main_sessions AS (
	select 
		row_number() over( order by ts.start_time) as id,
		l.country as tablet_country,
		ts.device_id,
		start_time::timestamp ,
		end_time::timestamp ,
		duration ,
		literacy_level ,
		numeracy_level ,
		l.id as location_id,
		l."name" as location,
		l.admin_3_name as admin_3_name,
		l.country,
        p.name as partner,
		s.name as field_officer,
		date_trunc('week', start_time::timestamp)::date as week,
		dt.term_id ,
		dt.term_name,
		tw.week_number as term_week
	from {{ref('stg_unique_sessions')}} ts 
	left join {{ref('stg_devices') }}  d on 
        (ts.device_id = d.serial_number ) or 
        (ts.device_id = d.device_id )  -- 2023.03.08 AP this logic handles the picking of the device_id OR serial_number for joining because the data from the tablets is often inconsistent
	left join {{ref('devices_per_term')}} dt on 
		d.serial_number = dt.tablet_serial_number and 
		ts.start_time::date >= dt.term_start::date and 
		ts.start_time::date <= dt.term_end::date
	left join {{ref('stg_locations')}} l on dt.location_id = l.id 
	left join {{ref('stg_term_weeks')}} tw on 
		dt.term_id = tw.term_id and
		date_trunc('week',(ts.start_time::date)) = tw.week  
	left join {{ref('stg_staff')}} s on s.id = l.staff_id
    left join {{ ref('stg_partners') }} p on s.partner_id::int = p.id	
    order by device_id ,session_id
), 
recent_data AS (
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
	ms.start_time::timestamp,
	ms.end_time::timestamp,
	ms.duration,
	ms.literacy_level,
	ms.numeracy_level,
	ms.location_id,
	ms.location,
	ms.admin_3_name,
	ms.country,
    ms.partner,
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

{# need to fix this: pick only the 1st instance of a session for each device. Then insert new ones. 
{{
    config(
        materialized='incremental'
    )
}}

select
    *,
    my_slow_function(my_column)

from raw_app_data.events

{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  where event_time > (select max(event_time) from {{ this }})

{% endif %}
#}