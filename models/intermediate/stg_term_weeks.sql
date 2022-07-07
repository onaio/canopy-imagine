select 
t.id as term_id, 
t.name as term_name, 
t.country as term_country,
t.start_date ,
t.end_date,
w.week,
row_number() over (partition by t.id order by w.week) as week_number
from {{ref('stg_terms')}} t 
left join reference.weeks w on t.start_date::date < w.week and t.end_date::date > w.week