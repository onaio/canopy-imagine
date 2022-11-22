with tablet_inventory as (
    select
        term_id, 
        location_id,
        term_week,
        actual as location_tablets
    from {{ref('inventory_weekly')}}
    where inventory_type = 'tablet'
), max_week as (
    select
        term_id, 
        location_id,
        max(term_week) as max_term_week
    from tablet_inventory
    group by 1,2
)

select 
    le.id, 
    le.term_id, 
    t.name as term_name,
    le.location_id,
    l.name as location,
    l.country,
    le.grade_id, 
    g.name as grade,
    le.number,
    ti.location_tablets,
    case when t.latest_term = true then 'Yes' else 'No' end as is_latest_term
from 
{{ref('stg_location_enrollments')}} le 
left join {{ref('stg_terms')}} t on le.term_id = t.id
left join {{ref('locations')}} l on le.location_id = l.id
left join {{ref('stg_grades')}} g on le.grade_id = g.id
left join max_week mw on mw.term_id = le.term_id and mw.location_id = le.location_id
left join tablet_inventory ti on ti.term_id = le.term_id and ti.location_id = le.location_id and ti.term_week = mw.max_term_week