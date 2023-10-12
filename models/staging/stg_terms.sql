with latest_term_per_country as (
    select
    c.name as country,
    max(t.id) filter (where start_date::date <= now()::date ) as latest_term
    from  {{source('directus', 'terms')}} t
    left join {{source('directus', 'partners_countries')}} pc on pc.id = t.partner_country_id
    left join {{source('directus', 'countries')}} c on c.id = pc.countries_id
    group by 1
)


select 
    t.id::int,
    partner_country_id as partner_id,
    rtrim(c.name) as country,
    rtrim(t.name) as name,
    start_date::date,
    end_date::date,
    case when lt.latest_term is not null then true else false end as latest_term,
    case when (now()::date >= start_date::date and now()::date <= end_date::date) then true else false end as  current_term
from {{source('directus', 'terms')}} t
left join latest_term_per_country lt  on t.id = lt.latest_term
left join {{source('directus', 'partners_countries')}} pc on pc.id = t.partner_country_id
left join {{source('directus', 'countries')}} c on c.id = pc.countries_id
