
------ reporting.inventory_weekly
--1. find different inventory types
{%- set inventory_types = ['tablet', 'headset', 'charge cable', 'screen protector'] -%}
{%- set inventory_field_prefix = ['tablets', 'headsets', 'cables', 'protectors'] -%}

--2. inventory table
with inventory_table as (
select
	l.id as location_id,
	l.name as location,
	l.country,
	tw.term_id,
	tw.week as week,
    tw.week_number as term_week, 
	tw.term_name,
	ia.inventory_type,
	ia.quantity as expected,
	{%- for movement in ['delivered', 'decommissioned', 'to_replace'] -%}
	case 	--for each inventory type, check added,removed,to replace.  
		{% for i in range(0, inventory_types | length) %}
		when ((ia.inventory_type = '{{inventory_types[i]}}') and '{{movement}}' = 'delivered' and tw.week_number = 1) then ia.quantity::int
		when ((ia.inventory_type = '{{inventory_types[i]}}')) then ms.{{inventory_field_prefix[i]}}_{{movement}}
		{%- endfor %}
	end as {{movement}}, 
	{%- endfor -%}
	'test' as test
from {{ref('stg_term_weeks')}} tw
left join {{ref('stg_locations')}} l on tw.term_country = l.country 
left join {{ref('monitoring_survey')}} ms on ms.week = tw.week and tw.term_id = ms.term_id and ms.location_id = l.id
left join {{source('airbyte', 'inventory_allocation')}} ia on ia.location_id = l.id and ia.term_id = tw.term_id
),
-- 3. Weekly inventory table. Used if there are more than one survey for one location per week 
weekly_inventory as (
select
	location_id,
	location,
	country,
	term_id,
    week,
	term_week,
	term_name,
	inventory_type,
	expected,
	sum(coalesce(delivered,0)) as delivered,
	sum(coalesce(decommissioned,0)) as decommissioned,
	sum(coalesce(to_replace,0)) as to_replace
from inventory_table  
group by 1,2,3,4,5,6,7,8,9
)

-- 4. Final table with weekly updates
select 
	row_number() over( order by location_id) as id,
	location_id,
	location,
	country,
	term_id,
    week,
	term_week,
	term_name,
	inventory_type,
	expected,
	(sum(coalesce(delivered,0)-coalesce(decommissioned,0)) over (partition by location_id, inventory_type, term_id order by term_week)) as actual,
	delivered,
	decommissioned,
	to_replace
from weekly_inventory i 
order by term_id, location_id, term_week
