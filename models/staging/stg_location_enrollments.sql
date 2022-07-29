select 
row_number() over () as id, 
* 
from {{source('airbyte','location_enrollments')}}