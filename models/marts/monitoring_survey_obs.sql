select 
    submission_id, 
    field_officer,
    location, 
    country,
    admin_3_name, 
    observation_date,
    unnest(array['setup', 'tablet_use', 'closing']) as observation_time,
    unnest(array[setup_observations, tablet_use_observations, closing_observations]) as observations
from {{ref('monitoring_survey')}}