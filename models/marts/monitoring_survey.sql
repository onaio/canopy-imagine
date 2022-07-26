with tablet_delivery as (
select 
	tr.parent_id,
	count (tr.distributed_tablets) as tablets_delivered,
	string_agg(tr.distributed_tablets, ',') as tablet_ids_delivered
from {{ref('stg_monitoring_survey_tablet_repeat')}}  tr
group by 1

), tablet_decommissioned AS (
select 
	td.parent_id,
	count (td.decommissioned_tablets) as tablets_decommissioned,
	string_agg(td.decommission_date::VARCHAR, ',') as tablets_decommission_date,
	string_agg(td.decommissioned_tablets, ',') as tablet_ids_decommissioned
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
t.tablets_delivered,
t.tablet_ids_delivered,
ms.headsets_delivered,
ms.cables_delivered,
ms.protectors_delivered,
ms.decommission_tablets, 
ms.other_decommission,
d.tablets_decommissioned,
d.tablet_ids_decommissioned,
ms.headsets_decommissioned,
ms.cables_decommissioned,
ms.protectors_decommissioned,
ms.replacements,
ms.tablets_to_replace,
ms.headsets_to_replace,
ms.cables_to_replace,
ms.protectors_to_replace,
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