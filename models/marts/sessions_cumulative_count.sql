with cumulative_calc as (
select 
	tw.week,
	tw.week_number as term_week,
	tw.term_id,
	se.location_id,
    COUNT( se.id) over (partition by se.location_id, tw.term_id order by tw.week) as cumulative_sessions
from {{ref('stg_term_weeks')}} tw
left join {{ref('sessions')}} se on date_trunc('week',(se.start_time::date)) = tw.week and tw.term_id = se.term_id 
where tw.week <= current_date 
group by 1,2,3,4, se.id
)

select 
    week,
	term_week,
	term_id,
	location_id,
    cumulative_sessions
from cumulative_calc
group by 1,2,3,4,5
order by week, location_id
