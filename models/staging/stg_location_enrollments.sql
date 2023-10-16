select 
    row_number() over () as id, 
    term_id::int,
    grade_id::int,
    location_id::int, 
    quantity as number 
from {{source('directus','location_enrollments')}}