{{ config(store_failures = true) }}

select 
    *
from {{ref("int_weekly_monitoring_survey")}}
where iwon_school_id = ''