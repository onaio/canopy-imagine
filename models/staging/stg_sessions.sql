
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
    nullif(study_units,'')::int as study_units,
    nullif(playzone_units,'')::int as playzone_units,
    nullif(library_units, '')::int as library_units,
    nullif(diagnostic_time, '')::int as diagnostic_time,
    nullif(study_time,'')::int as study_time,
    nullif(playzone_time,'')::int as playzone_time,
    nullif(literacy_time,'')::int as literacy_time,
    nullif(numeracy_time,'')::int as numeracy_time,
    nullif(literacy_level,'')::int as literacy_level,
    nullif(numeracy_level,'')::int as numeracy_level
from {{source('csv', 'tablet_sessions')}}
