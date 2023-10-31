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
    ws.cumulative_sessions,
	ws.reporting_devices,
    ia.quantity as allocated_devices,
	ws.session_records,
	ws.actual_mins,
	ws.expected_mins
from {{ref("stg_locations")}} l
left join {{ref("int_sessions_weekly")}} ws on l.id = ws.location_id
left join {{ref("stg_inventory_allocation")}} ia on l.id = ia.location_id and ws.term_id = ia.term_id and ia.inventory_type = 'tablet'
