with device_info as (
    select
        (d._airbyte_data->>'peer_id') as peer_id,
        d._airbyte_data->>'device_id' as device_id,
        d._airbyte_data->>'serial' as serial_number
    from {{source('eidu', '_airbyte_raw_device_info')}} d
)

select
    distinct on (d.peer_id) peer_id,
    d.device_id,
    d.serial_number
from device_info d
