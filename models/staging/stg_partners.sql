select
    id::int,
    name,
    comment,
    last_modified_at
from {{ source('airbyte', 'partners') }}