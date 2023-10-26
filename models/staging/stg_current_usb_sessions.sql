{% set importedfields = ['study_units', 'playzone_units','library_units', 'diagnostic_time', 'study_time', 
    'playzone_time', 'literacy_time', 'numeracy_time', 'literacy_level', 'numeracy_level'] %}

select 
    rtrim(device_id) as device_id,
    round(user_id::real) as user_id, 
    round(session_id::real) as session_id, 
    rtrim(code) as code,
    rtrim(mode) as mode,
    rtrim(language) as language,
    start_time::timestamp,
    end_time::timestamp,
    duration,
    {% for field in importedfields %}
        round({{field}}::real) as {{field}}
        {%- if not loop.last -%}
        ,    
        {%- endif -%}
    {% endfor %}
from {{source('device_logs', 'usb_sessions')}} 