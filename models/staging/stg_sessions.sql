{% set importedfields = ['study_units', 'playzone_units','library_units', 'diagnostic_time', 'study_time', 
    'playzone_time', 'literacy_time', 'numeracy_time', 'literacy_level', 'numeracy_level'] %}

select 
    country,
    device_id,
    case when user_id = '' then NULL::int else round(user_id::real) end as user_id, 
    case when session_id = '' then NULL::int else round(session_id::real) end as session_id, 
    code,
    mode, 
    language,
    start_time::timestamp,
    end_time::timestamp,
    duration,
    {% for field in importedfields %}
        round(nullif({{field}},'')::real) as {{field}}
        {%- if not loop.last -%}
        ,    
        {%- endif -%}
    {% endfor %}
from {{source('csv', 'tablet_sessions')}} 
where start_time ='2022-09-21 12:59'
