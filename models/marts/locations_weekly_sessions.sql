--2023.10.31 Might need to replace with _int_sessions_weekly entirely

{{
    config(
        materialized='table'
    )
}}

select
    l.id as location_id, 
    l.lat::float,
    l.lon::float,
    l.name as location,
    l.type,
    l.country,
    l.admin_3_name,
    ws.week,
	ws.term_week,
	ws.term_id,
	ws.term_name,
    ws.partner,
	ws.field_officer,
	ws.is_last_week,
	ws.children,
	ws.reporting_devices,
    ws.allocated_devices,
	ws.session_records,
	ws.actual_mins,
	ws.expected_mins
from {{ref("int_locations_weekly")}} ws
left join {{ref("stg_locations")}} l on l.id = ws.location_id
