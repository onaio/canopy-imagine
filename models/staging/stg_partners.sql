select
    id::int,
    rtrim(name) as name,
    rtrim(comment) as comment
from {{ source('directus', 'partners') }}