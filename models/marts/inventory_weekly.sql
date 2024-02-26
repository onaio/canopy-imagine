
------ reporting.inventory_weekly
--1. find different inventory types
{%- set inventory_types = ['tablet', 'headset', 'headset cable', 'screen protector'] -%}
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
    ms.partner,
	ms.field_officer,
	ia.inventory_type,
	ia.quantity as expected,
	{%- for movement in ['delivered', 'decommissioned', 'lost', 'broken'] -%}
	max(case 	--for each inventory type, check added,removed,to replace.  
		{% for i in range(0, inventory_types | length) %}
		when ((ia.inventory_type = '{{inventory_types[i]}}') and '{{movement}}' = 'delivered' and tw.week_number = 1) then ia.quantity::int
		when ((ia.inventory_type = '{{inventory_types[i]}}')) then ms.{{inventory_field_prefix[i]}}_{{movement}}
		{%- endfor %}
	end) as {{movement}}, 
	{%- endfor -%}
	'test' as test,
	case when tw.latest_term = true then 'Yes' else 'No' end as is_latest_term,
	l.date_launched
from {{ref('stg_term_weeks')}} tw
left join {{ref('stg_locations')}} l on tw.term_country = l.country 
left join {{ref('monitoring_survey')}} ms on ms.week = tw.week and tw.term_id = ms.term_id and ms.location_id = l.id
left join {{ref('stg_inventory_allocation')}} ia on ia.location_id = l.id and ia.term_id = tw.term_id
group by 1,2,3,4,5,6,7,8,9,10,11,16,17,18
), 
-- 3. Get most recent survey data dates
recent_survey_data AS (
	select
		location_id,
		field_officer,
		MAX(date_trunc('week', week::timestamp)::date) as most_recent_date
	from inventory_table
	group by 1,2
),
-- 4. Weekly inventory table. Used if there are more than one survey for one location per week 
weekly_inventory as (
select
	it.location_id,
	it.location,
	it.country,
	it.term_id,
    it.week,
	it.term_week,
	it.term_name,
    it.partner,
	it.field_officer,
	it.is_latest_term,
	case when date_trunc('week', it.week::timestamp)::date = rd.most_recent_date then 'Yes' else 'No' end as is_last_week,
	it.inventory_type,
	it.expected,
	it.date_launched,
	sum(coalesce(it.delivered,0)) as delivered,
	sum(coalesce(it.decommissioned,0)) as decommissioned,
	sum(coalesce(it.lost,0)) as lost,
	sum(coalesce(it.broken,0)) as broken
	
from inventory_table it
left join recent_survey_data rd on rd.location_id = it.location_id and rd.field_officer = it.field_officer  
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14
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
    partner,
	field_officer,
	inventory_type,
	expected,
	date_launched,
	(sum(delivered - decommissioned) over (partition by location_id, inventory_type, term_id order by term_week)) as actual,
	case when term_week = 1 then 0 else delivered end as delivered,   -- accounts for the fact that the week1 deliveries are not happening in the field
	decommissioned,
	lost,
	broken,
	sum(case when term_week = 1 then 0 else delivered end
		) over (partition by location_id, inventory_type, term_id order by term_week) as cumulative_delivered,
	sum(decommissioned) over (partition by location_id, inventory_type, term_id order by term_week) as cumulative_decommissioned, 
	sum(lost) over (partition by location_id, inventory_type, term_id order by term_week) as cumulative_lost, 
	sum(broken) over (partition by location_id, inventory_type, term_id order by term_week) as cumulative_broken,
	is_latest_term,
	is_last_week
	
from weekly_inventory i 
where inventory_type is not null 
order by term_id, location_id, term_week