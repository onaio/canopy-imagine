with main as (
    {{ dbt_utils.union_relations(
        relations=[ref('stg_historical_usb_sessions'), ref('stg_current_usb_sessions'), ref('stg_filtered_eidu_sessions')]
    ) }}
), main_coalesce as(
    select
        _dbt_source_relation,
        device_id,
        user_id,
        session_id,
        code,
        mode,
        language,
        start_time,
        end_time,
        duration,
        study_units,
        playzone_units,
        library_units,
        diagnostic_time,
        study_time,
        playzone_time,
        literacy_time,
        numeracy_time,
        library_time,
        literacy_level,
        numeracy_level,
        session_unique_id,
        coalesce(processed_at, _airbyte_emitted_at) as processed_at
    from main
), sessions_dedup as (
    {{ dbt_utils.deduplicate(   
        relation='main_coalesce',
        partition_by='session_unique_id',
        order_by='processed_at desc',
    )
    }}
)

select * 
from sessions_dedup
{% if is_incremental() %}
-- this filter will only be applied on an incremental run
where processed_at > (select max(processed_at) from {{ this }})
{% endif %}
