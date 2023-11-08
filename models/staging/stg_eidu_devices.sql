{# {{
    config(
        materialized='incremental'
    )
}} #}

with device_info as (
    select
        peer_id as peer_id,
        device_id as device_id,
        serial as serial_number,
        _airbyte_emitted_at
    from {{source('device_logs', 'devices')}}
), device_dedup as (
    {{ dbt_utils.deduplicate(
        relation='device_info',
        partition_by='peer_id, device_id',
        order_by='_airbyte_emitted_at desc',
    )
    }}
)

select *
from device_dedup
{# {% if is_incremental() %}

  where _airbyte_emitted_at > (select max(_airbyte_emitted_at) from {{ this }})

{% endif %} #}