select
    s.session_unique_id,
    s.device_id,
    s.start_time,
    u.*
from  {{ref("stg_device_units")}} u
left join {{ref("stg_eidu_sessions")}} s on u.session_id = s.session_id
where s.start_time >= date_trunc('week', current_date - interval '2 weeks')
  and s.start_time < date_trunc('week', current_date)