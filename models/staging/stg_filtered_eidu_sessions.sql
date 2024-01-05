{{
    config(
        materialized='incremental'
    )
}}

select
    s.*
from {{ref("stg_eidu_sessions")}} s
left join {{ ref('stg_devices_per_week') }} dtd 
    on 
    (s.device_id = dtd.device_id ) 
       and date_trunc('week', s.start_time::timestamp)::date = dtd.week
where 
    dtd.location_id not in (select location_id::int from {{source('data_corrections', 'schools_data_removal')}} )
    {% if is_incremental() %}
        and s._airbyte_emitted_at > (select max(_airbyte_emitted_at) from {{ this }})
    {% endif %}
