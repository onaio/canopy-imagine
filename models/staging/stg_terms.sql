select 
id::int,
country,
name,
start_date::date,
end_date::date
from 
{{source('airbyte', 'terms')}}