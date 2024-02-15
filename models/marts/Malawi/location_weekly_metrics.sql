select
    school_id,
    country,
    school_name,
    partner_name,
    observation_week,
    avg(minutes_used) as minutes_used,
    avg(absent_students) as percentage_absent
from {{ref("weekly_monitoring_survey")}}
group by 1,2,3,4,5
