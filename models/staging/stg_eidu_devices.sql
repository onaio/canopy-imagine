{{
    config(
        materialized='incremental'
    )
}}

with device_info as (
    select
        peer_id as peer_id,
        device_id as device_id,
        serial as serial_number,
        _airbyte_emitted_at
    from {{source('device_logs', 'devices')}}
)

select
    distinct on (peer_id) peer_id,
    device_id,
    serial_number,
    _airbyte_emitted_at
from device_info

{% if is_incremental() %}

  where _airbyte_emitted_at > (select max(_airbyte_emitted_at) from {{ this }})

{% endif %}