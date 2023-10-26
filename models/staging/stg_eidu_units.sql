{{
    config(
        materialized='incremental'
    )
}}

{% set units = ['diagnostic', 'study', 'library'] %}
{% set topics = ['literacy', 'numeracy'] %}


{% if is_incremental() %}

  select
    session_id,
    {% for unit in units %}
        sum(case when u.data::json->>'type' = '{{unit}}' then u.duration else 0 end) as {{unit}}_time,
    {% endfor %}
    {% for topic in topics %}
        sum(case when (u.data::json->>'unit_topic' = '{{topic}}') and (u.data::json->>'type' = 'study') then u.duration else 0 end) as {{topic}}_time,
    {% endfor %}
    max(_airbyte_emitted_at) as _airbyte_emitted_at
  from {{source('device_logs', 'units')}} u
  where _airbyte_emitted_at > (select max(_airbyte_emitted_at) from {{ this }})
  group by 1

{% endif %}
