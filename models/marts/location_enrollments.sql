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
case when t.latest_term = true then 'Yes' else 'No' end as is_latest_term
from 
{{ref('stg_location_enrollments')}} le 
left join {{ref('stg_terms')}} t on le.term_id = t.id
left join {{ref('locations')}} l on le.location_id = l.id
left join {{ref('stg_grades')}} g on le.grade_id = g.id