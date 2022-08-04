select 
row_number() over () as id, 
term_id::int,
grade_id::int,
location_id::int, 
number 
from {{source('airbyte','location_enrollments')}}