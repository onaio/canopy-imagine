with survey_intermediate as (
    select 
        tw.week,
        tw.week_number as term_week,
        tw.term_id,
        tw.term_name,
        l.country,
        l.lat,
        l.lon,
        p.name as partner,
        f.name as field_officer,
        l.id as location_id,
        l."name" as location,
        l.date_launched,
        SUM(le.number::INT) as total_enrollment,
        COUNT(DISTINCT ms.id) as visits
    from {{ref('stg_term_weeks')}} tw
    left join {{ref('stg_locations') }} l on l.country = tw.term_country
    left join {{ref('stg_location_enrollments') }} le on le.location_id = l.id and le.term_id = tw.term_id
    left join {{ref('stg_monitoring_survey')}}  ms on date_trunc('week',(ms.observation_date::date)) = tw.week and ms.location_id = l.id::VARCHAR 
    left join {{ref('stg_staff') }}  f on f.id = l.staff_id
    left join {{ ref('stg_partners') }} p on f.partner_id::int = p.id
    group by 1,2,3,4,5,6,7,8,9,10,11,12, ms.id, ms.start 
    order by ms.id, ms.start 
)

select
    week,
    term_week,
    term_id,
    term_name,
    country,
    location_id,
    location,
    lat,
    lon,
    partner,
    field_officer,
    total_enrollment,
    visits,
    case when visits >= 1 then 'Yes' else 'No' end as visit_confirmed,
    date_launched
from survey_intermediate