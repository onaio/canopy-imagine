with main as (
    {{ dbt_utils.union_relations(
        relations=[ref('stg_historical_usb_sessions'), ref('stg_current_usb_sessions')]
    ) }}
 ) 
 

 select 
    ROW_NUMBER() OVER(PARTITION BY session_unique_id ORDER BY start_time asc) AS row_copy,
    session_unique_id,
    device_id, 
    session_id,
    mode, 
    language,
    start_time,
    end_time,
    (literacy_time + numeracy_time + playzone_time +  diagnostic_time)::int as duration,
    study_units,
    playzone_units,
    library_units,
    diagnostic_time,
    study_time,
    playzone_time as library_time,
    literacy_time,
    numeracy_time,
    literacy_level,
    numeracy_level
from main