select
    location_id,
    country,
    location,
    partner,
    observation_week,
    avg(minutes_used::int) as minutes_used,
    avg(absent_students::int) as percentage_absent
from {{ref("weekly_monitoring_survey")}}
group by 1,2,3,4,5
