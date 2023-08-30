{% set units = ['diagnostic', 'study', 'library'] %}
{% set topics = ['literacy', 'numeracy'] %}

with main as (
    select
        s._airbyte_data->>'session_id' as session_id,
        u._airbyte_data->'data'->>'unit_topic' as unit_topic,
        u._airbyte_data->'data'->>'type' as unit_type,
        (u._airbyte_data->>'duration')::INT as unit_duration
    from {{source('eidu', '_airbyte_raw_session')}}  s
    left join {{source('eidu', '_airbyte_raw_unit')}} u ON s._airbyte_data->>'session_id' = u._airbyte_data->>'session_id'
), unit_types as (
    select
        session_id,
        {{ dbt_utils.pivot(
            'unit_type',
            units,
            agg='sum',
            then_value='unit_duration'
        ) }}
    from main
    group by session_id
), unit_topics as (
    select
        session_id,
        {{ dbt_utils.pivot(
            'unit_topic',
            topics,
            agg='sum',
            then_value='unit_duration'
        ) }}
    from main
    group by session_id   
),  device_info as (
    select
        (d._airbyte_data->>'peer_id') as peer_id,
        d._airbyte_data->>'device_id' as device_id,
        d._airbyte_data->>'serial' as serial_number
    from {{source('eidu', '_airbyte_raw_device_info')}} d
), devices as (
    select
        distinct on (d.peer_id) peer_id,
        d.device_id,
        d.serial_number
    from device_info d
)



select
    d.device_id,
    d.serial_number,
    s._airbyte_data->>'session_id' as session_id,
    s._airbyte_data->>'mode' as mode,
    s._airbyte_data->>'lang' as language,
    s._airbyte_data->>'start_time' as start_time,
    s._airbyte_data->>'end_time' as end_time,
    (s._airbyte_data->>'duration')::INT as duration,
    {% for unit in units %}
        ut.{{unit}} as {{unit}}_time,
    {% endfor %}
    {% for topic in topics %}
        tpc.{{topic}} as {{topic}}_time
        {%- if not loop.last -%}
        ,    
        {%- endif -%}
    {% endfor %}
from {{source('eidu', '_airbyte_raw_session')}}  s
left join devices d ON s._airbyte_data->>'peer_id' = d.peer_id
left join unit_types ut on s._airbyte_data->>'session_id' = ut.session_id
left join unit_topics tpc on s._airbyte_data->>'session_id' = tpc.session_id