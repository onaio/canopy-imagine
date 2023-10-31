-- Model with each row representing one device in one term week. Applies to all devices that have been allocated to schools in a term. 

{{
    config(
        materialized='table'
    )
}}

select 
    dt.id,
    dt.device_id,
    dt.serial_number,
    dt.location_id,
    dt.location,
    dt.admin_3_name,
    dt.country,
    dt.partner,
    dt.field_officer,
    dt.term_id ,
    dt.term_name,
    tw.week,
    tw.week_number
from 
{{ref('devices_per_term')}} dt  
left join {{ ref('stg_term_weeks') }} tw on dt.term_id = tw.term_id 
