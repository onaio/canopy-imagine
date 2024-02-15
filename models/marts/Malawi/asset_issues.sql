with identified_issues as (
    select
        school_id,
        country,
        school_name,
        partner_name,
        observation_date,
        observation_week,
        replace(regexp_split_to_table(identified_issues , ' '), '_', ' ') as identified_issue
    from {{ref("weekly_monitoring_survey")}}
    where identified_issues != ''
)

select
    school_id,
    country,
    school_name,
    partner_name,
    max(observation_date) as observation_date,
    observation_week,
    identified_issue
from identified_issues
group by 1,2,3,4,6,7


