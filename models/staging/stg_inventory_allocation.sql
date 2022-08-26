select 
term_id::int,
location_id::int,
inventory_type,
quantity::int
from 
{{source('airbyte', 'inventory_allocation')}}