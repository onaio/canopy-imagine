with latest_term_per_country as (
    select
    country, max(id) filter (where start_date::date <= now()::date ) as latest_term
    from  {{source('airbyte', 'terms')}}
    group by 1
)
select 
id::int,
partner_id,
t.country,
name,
start_date::date,
end_date::date,
case when lt.latest_term is not null then true else false end as latest_term,
case when (now()::date >= start_date::date and now()::date <= end_date::date) then true else false end as  current_term
from 
{{source('airbyte', 'terms')}} t
left join latest_term_per_country lt  on t.id = lt.latest_term
