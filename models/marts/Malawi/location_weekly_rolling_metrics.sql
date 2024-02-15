select
    l.id as location_id,
    rw.week,
    coalesce(avg(session_rating::int),0) as rolling_session_rating
from {{ref("reference_weeks")}} rw
cross join (select distinct id from {{ref("locations")}}) l
left join {{ref("weekly_monitoring_survey")}} ms on ms.observation_date < (rw.week + interval '6 days')::date
    and ms.observation_date >= rw.week - interval '5 weeks'
    and ms.school_id::int = l.id
where rw.week >= '2024-01-01' and rw.week <= current_date
group by 1,2
window w as (partition by l.id, rw.week order by rw.week rows between 5 preceding and current row)