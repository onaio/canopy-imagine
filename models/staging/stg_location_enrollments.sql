select 
    row_number() over () as id, 
    term_id::int -- partner_term_id
    grade_id::int,
    location_id::int, 
    quantity as number 
from {{source('directus','location_enrollments')}}