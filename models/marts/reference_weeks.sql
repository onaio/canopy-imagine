select row_number() over () as id, week::date as week
from 
(
select
	(generate_series(min, max, '1 week'))::date as week
	FROM (
	  SELECT date_trunc('week', min(start_date::date)) AS min, 
	         date_trunc('week', max(end_date::date)) AS max
	  FROM {{ref('stg_terms') }} sub	  
	  ) a
) foo
