select
    formid,
    school_id,
    country,
    school_name,
    partner_name,
    observation_date,
    observation_week,
    replace(regexp_split_to_table(feedback_to_teacher , ' '), '_', ' ') as feedback_to_teacher
from {{ref("weekly_monitoring_survey")}}
where feedback_to_teacher != ''
