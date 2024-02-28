select
    formid,
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
    replace(regexp_split_to_table(feedback_to_teacher , ' '), '_', ' ') as feedback_to_teacher
from {{ref("weekly_monitoring_survey")}}
where feedback_to_teacher != ''
