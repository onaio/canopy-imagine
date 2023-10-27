{{
    config(
        materialized='incremental',
        unique_key='id'
    )
}}

select *
from {{ref("sessions")}}
where country = 'Malawi'


{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  where processed_at > (select max(processed_at) from {{ this }})
{% endif %}
