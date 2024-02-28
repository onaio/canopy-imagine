with identified_issues as (
    select
        location_id,
        country,
        admin_3_name,
        location,
        date_launched,
        partner,
        observation_date,
        week,
        term_week,
        term_id,
        term_name,
        replace(regexp_split_to_table(identified_issues , ' '), '_', ' ') as identified_issue
    from {{ref("weekly_monitoring_survey")}}
    where identified_issues != ''
)

select
    location_id,
    country,
    location,
    date_launched,
    partner,
    max(observation_date) as observation_date,
    week,
    term_week,
	term_id,
	term_name,
    identified_issue
from identified_issues
group by 1,2,3,4,5,7,8,9,10,11


