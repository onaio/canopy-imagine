with usb_union as (
    {{ dbt_utils.union_relations(
        relations=[ref('stg_historical_usb_sessions'), ref('stg_current_usb_sessions')]
    ) }}
)

{{ dbt_utils.deduplicate(   
    relation='usb_union',
    partition_by='session_unique_id',
    order_by='processed_at desc',
)
}}