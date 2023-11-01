-- model with one row per school per term per grade

select 
    le.id, 
    le.term_id as partner_term_id, 
    t.name as term_name,
    le.location_id,
    l.name as location,
    l.admin_3_name,
    l.country,
    l.lat,
    l.lon,
    p.name as partner,
    le.grade_id, 
    g.name as grade,
    le.number,
    NULL as is_latest_term, -- 2023.10.31 AP simplifying for now. OG:   case when t.latest_term = true then 'Yes' else 'No' end as is_latest_term,
    f.name as field_officer 
from 
{{ref('stg_location_enrollments')}} le 
left join {{ref('stg_terms')}} t on le.term_id = t.id
left join {{ ref('stg_partners_countries') }} pc on t.partner_id = pc.id
left join {{ref('stg_partners') }} p on pc.partners_id = p.id  
left join {{ref('locations')}} l on le.location_id = l.id
left join {{ref('stg_staff') }} f on f.id = l.staff_id
left join {{ref('stg_grades')}} g on le.grade_id = g.id
