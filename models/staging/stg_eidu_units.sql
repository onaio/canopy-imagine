{# {{
    config(
        materialized='incremental',
        unique_key='session_id'
    )
}} #}

{% set units = ['diagnostic', 'study', 'library'] %}
{% set topics = ['literacy', 'numeracy'] %}

  select
    session_id,
    {% for unit in units %}
        sum(case when u.type = '{{unit}}' then u.duration else 0 end) as {{unit}}_time,
    {% endfor %}
    {% for topic in topics %}
        sum(case when (u.unit_topic = '{{topic}}') and (u.type = 'study') then u.duration else 0 end) as {{topic}}_time,
    {% endfor %}
    max(_airbyte_emitted_at) as _airbyte_emitted_at
  from {{ ref('stg_device_units') }} u

{#   {% if is_incremental() %}
  where _airbyte_emitted_at > (select max(_airbyte_emitted_at) from {{ this }})
{% endif %} #}
  group by 1

