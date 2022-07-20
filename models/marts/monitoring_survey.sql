-- work in progress!! 

with tablet_delivery as (
select 
	tr.parent_id,
	count (tr.distributed_tablets) as tablet_delivery,
	string_agg(tr.distributed_tablets, ',') as tablet_delivery_ids
from {{ref('stg_monitoring_survey_tablet_repeat')}}  tr
group by 1

), tablet_decommissioned AS (
select 
	td.parent_id,
	count (td.decommissioned_tablets) as decommissioned_tablets,
	string_agg(td.decommission_date::VARCHAR, ',') as decommissioned_tablet_dates,
	string_agg(td.decommissioned_tablets, ',') as decommissioned_tablet_ids
from {{ref('stg_monitoring_survey_decommission_repeat')}}  td
group by 1
)
select 
row_number() over( order by ms.id) as id,
ms.id as submission_id,
ms.uuid,
ms.start,
ms.end,
l.country,
f.name as field_officer,
l.id as location_id,
l."name" as location,
l.admin_3_name as admin_3_name,
ms.observation_date,
ms.setup_observations,
ms.tablet_use_observations,
ms.tablet_usage_mins,
ms.closing_observations,
ms.session_rating,
ms.other_session_obs,
ms.distribution,
ms.distributed_equipment,
t.tablet_delivery,
t.tablet_delivery_ids,
ms.headset_delivery,
ms.cable_delivery,
ms.protector_delivery,
ms.decommission_tablet,
ms.other_decommission,
d.decommissioned_tablets,
d.decommissioned_tablet_ids,
ms.decommissioned_headsets,
ms.decommissioned_cables,
ms.decommisioned_protectors,
ms.replacements,
ms.tablet_replacements,
ms.headset_replacements,
ms.cable_replacements,
ms.protector_replacements,
ms.other_replacement_obs,
ms.submitted_at,
ms.submission_submitted_by,
date_trunc('week', ms.observation_date::timestamp)::date as week, 
tw.term_id,
tw.term_name,
tw.week_number as term_week
from {{ref('stg_monitoring_survey')}}  ms 
left join {{ref('stg_locations') }} l on ms.location_id = l.id::VARCHAR 
left join {{ref('stg_term_weeks')}}  tw on 
	l.country = tw.term_country and
	date_trunc('week',(ms.observation_date::date)) = tw.week 
left join tablet_delivery t on t.parent_id = ms.id
left join tablet_decommissioned d on d.parent_id = ms.id
left join {{ref('stg_staff') }}  f on LTRIM(ms.field_officer, 'f') =f.id::VARCHAR
order by ms.id, ms.start 