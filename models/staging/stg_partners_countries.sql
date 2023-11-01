select * 
from {{ source('directus', 'partners_countries') }}