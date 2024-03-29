select 
id::bigint, 
uuid, 
start,
"end",
rtrim(country) as country,
rtrim(field_officer) as field_officer,
LTRIM(school_name, 's') as location_id , 
observation_date,
rtrim(setup_observations) as setup_observations, 
rtrim(tablet_use_observations) as tablet_use_observations, 
tablet_usage_mins,
rtrim(closing_observations) as closing_observations,
rtrim(session_rating) as session_rating,
rtrim(other_session_obs) as other_session_obs,
rtrim(distribution) as distribution,
distributed_equipment,
headset_delivery as headsets_delivered,
cable_delivery as cables_delivered,
protector_delivery as protectors_delivered,
rtrim(decommission_tablet) as decommission_tablets,
other_decommission as other_decommission, 
decommissioned_headsets as headsets_decommissioned,
decommissioned_cables as cables_decommissioned,
decommisioned_protectors as protectors_decommissioned,
replacements,
tablets_lost, 
tablets_broken,
headsets_lost,
headsets_broken, 
cables_lost,
cables_broken, 
protectors_lost,
protectors_broken,
rtrim(other_replacement_obs) as other_replacement_obs,
instance_id, 
modified_at,
submitted_at,
rtrim(submission_submitted_by) as submission_submitted_by
from {{source('ona_data','monitoring_crs_pilot')}}