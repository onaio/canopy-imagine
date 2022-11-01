select 
t.id as term_id, 
t.name as term_name, 
t.country as term_country,
t.start_date as term_start_date,
t.end_date as term_end_date,
t.latest_term,
t.current_term,
w.week,
w.week_number, 
d.day,
row_number() over (partition by t.id order by w.week_number, d.day) as day_number
from {{ref('stg_terms')}} t 
left join {{ref('stg_term_weeks')}} w on t.id = w.term_id
left join {{ref('reference_days')}} d on w.week = date_trunc('week', d.day)::date
order by t.id, w.week