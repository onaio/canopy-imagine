{% set importedfields = ['study_units', 'playzone_units','library_units', 'diagnostic_time', 'study_time', 
    'playzone_time', 'literacy_time', 'numeracy_time', 'literacy_level', 'numeracy_level'] %}

with main as (
select 
    rtrim(device_id) as device_id,
    round(user_id::real) as user_id, 
    round(session_id::real) as session_id, 
    rtrim(code) as code,
    rtrim(mode) as mode,
    rtrim(language) as language,
    start_time::timestamp,
    end_time::timestamp,
    {% for field in importedfields %}
        round({{field}}::real) as {{field}},
    {% endfor %}
    _airbyte_normalized_at as processed_at

from {{source('device_logs', 'usb_sessions')}} 
)

select 
    *,
    (literacy_time + numeracy_time + playzone_time + diagnostic_time)::int as duration,
    device_id || coalesce(code,'nocode') || start_time || coalesce((literacy_time + numeracy_time + playzone_time + diagnostic_time)::text,'') AS session_unique_id
from main
