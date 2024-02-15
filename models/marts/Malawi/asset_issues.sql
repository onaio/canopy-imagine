with identified_issues as (
    select
        location_id,
        country,
        location,
        partner,
        observation_date,
        observation_week,
        replace(regexp_split_to_table(identified_issues , ' '), '_', ' ') as identified_issue
    from {{ref("weekly_monitoring_survey")}}
    where identified_issues != ''
)

select
    location_id,
    country,
    location,
    partner,
    max(observation_date) as observation_date,
    observation_week,
    identified_issue
from identified_issues
group by 1,2,3,4,6,7


