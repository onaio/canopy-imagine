select row_number() over () as id, day::date as day
from 
(
select
	(generate_series(min, max, '1 day'))::date as day
	FROM (
	  SELECT date_trunc('day', min(start_date::date)) AS min, 
	         date_trunc('day', max(end_date::date)) AS max
	  FROM {{ref('stg_terms') }} ) sub	  
	  ) foo
