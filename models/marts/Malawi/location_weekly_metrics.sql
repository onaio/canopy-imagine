select
    location_id,
    country,
    admin_3_name,
    location,
    date_launched,
    partner,
    week,
    term_week,
	term_id,
	term_name,
    avg(minutes_used::int) as minutes_used,
    avg(absent_students::int) as percentage_absent
from {{ref("weekly_monitoring_survey")}}
group by 1,2,3,4,5,6,7,8,9,10
