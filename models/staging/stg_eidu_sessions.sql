{% set units = ['diagnostic', 'study', 'library'] %}
{% set topics = ['literacy', 'numeracy'] %}

with main as (
    select
        s.session_id,
        u.data::json->>'unit_topic' as unit_topic,
        u.data::json->>'type' as unit_type,
        u.duration as unit_duration
    from {{source('device_logs', 'sessions')}}  s
    left join {{source('device_logs', 'units')}} u ON s.session_id = u.session_id
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
    where unit_type = 'study'
    group by session_id   
), sessions as (
    select
        d.device_id,
        s.iw_session_id as session_id,
        s.mode as mode,
        s.lang as language,
        date_trunc('minute', to_timestamp((s.start_time)/1000)) as start_time,
        date_trunc('minute', to_timestamp((s.end_time)/1000)) as end_time,
        {% for unit in units %}
            ut.{{unit}} as {{unit}}_time,
        {% endfor %}
        {% for topic in topics %}
            tpc.{{topic}} as {{topic}}_time
            {%- if not loop.last -%}
            ,    
            {%- endif -%}
        {% endfor %}
    from {{source('device_logs', 'sessions')}}  s
    left join {{ref("stg_eidu_devices")}} d ON s.peer_id = d.peer_id
    left join unit_types ut on s.session_id = ut.session_id
    left join unit_topics tpc on s.session_id = tpc.session_id
)

select
    *,
    (literacy_time + numeracy_time + library_time +  diagnostic_time)::int as duration
from sessions