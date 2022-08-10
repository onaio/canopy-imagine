with data as (
select 
    submission_id, 
    field_officer,
    location, 
    country,
    admin_3_name, 
    observation_date,
    unnest(array['1. Setup', '2. During session', '3. Closing']) as observation_time,
    unnest(array[setup_observations, tablet_use_observations, closing_observations]) as observations
from {{ref('monitoring_survey')}}
)
select * from data 
where observations is not null and observations != 'na'
