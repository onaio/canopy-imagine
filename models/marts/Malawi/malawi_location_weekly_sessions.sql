select
    ws.*,
    coalesce(wrm.rolling_session_rating,0) as rolling_session_rating
from {{ref("locations_weekly_sessions")}} ws
left join {{ref("location_weekly_rolling_metrics")}} wrm on ws.location_id = wrm.location_id and ws.week = wrm.week
where country = 'Malawi'