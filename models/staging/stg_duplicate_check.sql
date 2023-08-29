 select 
    ROW_NUMBER() OVER(PARTITION BY device_id, code, start_time, duration ORDER BY start_time asc) AS row_copy,
    device_id || code || start_time || duration AS session_unique_id,
    device_id,
    user_id, 
    session_id, 
    code,
    mode, 
    language,
    start_time,
    end_time,
    (literacy_time + numeracy_time + playzone_time +  diagnostic_time)::int as duration,
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
from {{ref('stg_sessions')}}