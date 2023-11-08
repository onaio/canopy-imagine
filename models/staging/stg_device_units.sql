{# {{
    config(
        materialized='incremental',
        unique_key='session_id'
    )
}} #}

select
    session_id,
    u.data::json->>'type' as type,
	u.data::json->>'unit_topic' as unit_topic,
    u.duration,
    _airbyte_emitted_at as _airbyte_emitted_at
from  {{source('device_logs', 'units')}} u
{# {% if is_incremental() %}
  where _airbyte_emitted_at > (select max(_airbyte_emitted_at) from {{ this }})
{% endif %} #}
