select 
id::int, 
name,
comments 
from {{source('airbyte', 'grades')}}