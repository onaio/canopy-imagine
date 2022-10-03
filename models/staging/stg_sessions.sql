
select 
    country,
    device_id,
    case when user_id = '' then NULL::int else user_id::int end as user_id, 
    case when session_id = '' then NULL::int else session_id::int end as session_id, 
    code,
    mode, 
    language,
    start_time::timestamp,
    end_time::timestamp,
    duration::int,
    study_units,
    playzone_units,
    library_units,
    diagnostic_time,
    study_time,
    playzone_time,
    literacy_time,
    numeracy_time,
    literacy_level,
    numeracy_level
from {{source('csv', 'tablet_sessions')}}
