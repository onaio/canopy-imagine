{{ config(materialized='table') }}

with main as (
    {{ dbt_utils.union_relations(
        relations=[ref('stg_historical_usb_sessions'), ref('stg_current_usb_sessions'), ref('stg_eidu_sessions')]
    ) }}
 )
, duplicate_check as (
    select
        ROW_NUMBER() OVER(PARTITION BY session_unique_id ORDER BY processed_at desc) AS row_copy,
        main.*
    from main
)

select *
from duplicate_check
where row_copy = 1
