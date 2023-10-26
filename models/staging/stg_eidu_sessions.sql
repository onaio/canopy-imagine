select * from dbt_afuad.stg_eidu_sessions

{#
{{
    config(
        materialized='incremental'
    )
}}

select
    d.device_id,
    s.iw_session_id as session_id,
    s.mode as mode,
    s.lang as language,
    date_trunc('minute', to_timestamp((s.start_time)/1000)) as start_time,
    date_trunc('minute', to_timestamp((s.end_time)/1000)) as end_time,
    (u.literacy_time + u.numeracy_time + u.library_time + u.diagnostic_time)::int as duration,
    u.diagnostic_time,
    u.study_time,
    u.library_time,
    u.literacy_time,
    u.numeracy_time,
    s._airbyte_emitted_at as _airbyte_emitted_at
from {{source('device_logs', 'sessions')}} s
left join {{ref("stg_eidu_devices")}} d on s.peer_id = d.peer_id
left join {{ref("stg_eidu_units")}} u on s.session_id = u.session_id

{% if is_incremental() %}

  where s._airbyte_emitted_at > (select max(_airbyte_emitted_at) from {{ this }})

{% endif %}


#}