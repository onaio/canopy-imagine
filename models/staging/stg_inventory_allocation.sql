select 
term_id::int,
location_id::int,
rtrim(inventory_type) as inventory_type,
quantity::int
from 
{{source('airbyte', 'inventory_allocation')}}