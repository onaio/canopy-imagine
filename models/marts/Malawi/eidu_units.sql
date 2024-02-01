{{
    config(
        materialized='incremental'
    )
}}

select
    s.session_unique_id,
    s.device_id,
    s.start_time,
    u.*
from  {{ref("stg_device_units")}} u
left join {{ref("stg_eidu_sessions")}} s on u.session_id = s.session_id
{% if is_incremental() %}
  where u._airbyte_emitted_at > (select max(_airbyte_emitted_at) from {{ this }})
{% endif %}