{{
    config(
        materialized='incremental',
        unique_key='session_unique_id'
    )
}}

with main as (
    {{ dbt_utils.union_relations(
        relations=[ref('stg_historical_usb_sessions'), ref('stg_current_usb_sessions'), ref('stg_eidu_sessions')]
    ) }}
 ),
 main_filtered as 
 (select * from main 
 {% if is_incremental() %}
  -- this filter will only be applied on an incremental run
  where processed_at > (select max(processed_at) from {{ this }})
{% endif %}
), 
 
duplicate_check as (
    select
        ROW_NUMBER() OVER(PARTITION BY session_unique_id ORDER BY processed_at desc) AS row_copy,
        main.*
    from main_filtered main
)

select *
from duplicate_check
where row_copy = 1


