select 
id::int,
country,
metric,
value::int
from {{source('airbyte', 'country_metrics')}}