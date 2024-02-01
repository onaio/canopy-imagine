{% set columns = ['device_id', 'mode', 'language', 'start_time', 'end_time', 'duration', 'diagnostic_time', 'study_time', 'numeracy_time', 'session_unique_id'] %}

select
    {% for column in columns %}
        coalesce(u.{{column}}, e.{{column}}) as {{column}},
    {% endfor %}
    coalesce(u.playzone_time, e.library_time) as library_time,
    case when u.session_unique_id notnull then 'T' else 'F' end as usb,
    case when e.session_unique_id notnull then 'T' else 'F' end as eidu,
    coalesce(u.processed_at, e._airbyte_emitted_at) as processed_at
from {{ref("stg_usb_sessions_unique")}} u
full outer join {{ref("stg_eidu_sessions_unique")}} e on u.session_unique_id = e.session_unique_id

