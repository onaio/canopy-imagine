{{ config(materialized='table') }}

with main as (
    {{ dbt_utils.union_relations(
        relations=[ref('stg_unique_usb_sessions'), ref('stg_eidu_sessions')]
    ) }}
 )
--, duplicate_check as (
--     select
--         ROW_NUMBER() OVER(PARTITION BY device_id, session_id, start_time ORDER BY start_time asc) AS row_copy,
--         main.*
--     from main
-- )

-- select *
-- from duplicate_check
-- where row_copy = 1
select * from main