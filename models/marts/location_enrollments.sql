select 
le.id, 
le.term_id, 
t.name as term_name,
le.location_id,
l.name as location,
l.country,
le.grade_id, 
g.name as grade,
le.number 
from 
{{ref('stg_location_enrollments')}} le 
left join {{ref('stg_terms')}} t on le.term_id = t.id
left join {{ref('locations')}} l on le.location_id = l.id
left join {{ref('stg_grades')}} g on le.grade_id = g.id