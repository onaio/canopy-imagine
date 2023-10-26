with device_info as (
    select
        peer_id as peer_id,
        device_id as device_id,
        serial as serial_number
    from {{source('device_logs', 'devices')}}
)

select
    distinct on (peer_id) peer_id,
    device_id,
    serial_number
from device_info
