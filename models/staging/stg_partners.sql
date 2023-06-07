select
    id::int,
    rtrim(name) as name,
    rtrim(comment) as comment,
    last_modified_at
from {{ source('airbyte', 'partners') }}