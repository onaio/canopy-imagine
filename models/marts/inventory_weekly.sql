------ reporting.inventory_weekly
-- needs testing with updated dataset due to missing records
with inventory_table as (
select
	ms.school_id as location_id,
	ms.school as location_name,
	tw.term_id,
	tw.week as week,
    tw.week_number as term_week, 
	ia.inventory_type,
	ia.quantity as expected,
	case when ia.inventory_type = 'tablet' then ms.tablet_delivery   ---2022.07.20 AP. Can't say i love this case statement. 
		when ia.inventory_type = 'headset' then ms.headset_delivery
		when ia.inventory_type = 'charge cable' then ms.cable_delivery
		when ia.inventory_type = 'screen protector' then ms.protector_delivery else null end as added,
	case when ia.inventory_type = 'tablet' then ms.decommissioned_tablets 
		when ia.inventory_type = 'headset' then ms.decommissioned_headsets
		when ia.inventory_type = 'charge cable' then ms.decommissioned_cables
		when ia.inventory_type = 'screen protector' then ms.decommisioned_protectors else null end as removed,
	case when ia.inventory_type = 'tablet' then ms.tablet_replacements 
		when ia.inventory_type = 'headset' then ms.headset_replacements
		when ia.inventory_type = 'charge cable' then ms.cable_replacements
		when ia.inventory_type = 'screen protector' then ms.protector_replacements else null end as needs_replacement
from {{ref('stg_term_weeks')}} tw
left join {{ref('monitoring_survey')}} ms on ms.week = tw.week and tw.term_id = ms.term_id 
left join {{source('airbyte', 'inventory_allocation')}} ia on ia.location_id = ms.school_id and ia.term_id = tw.term_id
)

select 
	row_number() over( order by location_id) as id,
	location_id,
	location_name,
	term_id,
    week,
	term_week,
	inventory_type,
	expected,
	(sum(added-removed) over (partition by inventory_type order by term_week)) as actual,
	added,
	removed,
	needs_replacement
from inventory_table
where location_id is not null
