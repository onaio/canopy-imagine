select 
id::int, 
rtrim(name) as name,
rtrim(comments) as comments 
from {{source('airbyte', 'grades')}}