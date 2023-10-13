select 
id::int, 
rtrim(name) as name,
rtrim(description) as comments 
from {{source('directus', 'grades')}}