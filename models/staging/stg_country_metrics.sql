select 
id::int,
rtrim(country) as country,
rtrim(metric) as metric,
value::int
from {{source('airbyte', 'country_metrics')}}