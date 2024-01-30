{{
    config(
        materialized='table'
    )
}}

{{ dbt_utils.deduplicate(   
    relation=ref('stg_filtered_eidu_sessions'),
    partition_by='session_unique_id',
    order_by='_airbyte_emitted_at desc',
)
}}