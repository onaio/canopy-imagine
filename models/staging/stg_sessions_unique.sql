select
    {{ dbt_utils.star(from=ref('stg_duplicate_check'), except=["row_copy"]) }}
from {{ref('stg_duplicate_check')}} 
where row_copy = 1