{% set importedfields = ['study_units', 'playzone_units','library_units', 'diagnostic_time', 'study_time', 
    'playzone_time', 'literacy_time', 'numeracy_time', 'literacy_level', 'numeracy_level'] %}

with main as (
select 
    rtrim(device_id) as device_id,
    case when user_id = '' then NULL::int else round(user_id::real) end as user_id, 
    case when session_id = '' then NULL::int else round(session_id::real) end as session_id, 
    rtrim(code) as code,
    rtrim(mode) as mode, 
    rtrim(language) as language,
    start_time::timestamp,
    end_time::timestamp,
    {% for field in importedfields %}
        round(nullif({{field}},'')::real) as {{field}},
    {% endfor %}
    coalesce(processed_at, '1970-01-01'::timestamp) as processed_at
from {{source('csv', 'tablet_sessions')}} 
)

select
    *,
    (literacy_time + numeracy_time + playzone_time + diagnostic_time)::int as duration,
    device_id || code || start_time || coalesce((literacy_time + numeracy_time + playzone_time + diagnostic_time)::text,'') as session_unique_id
from main
