select 
    mt.id::int,
    rtrim(c.name) as country,
    rtrim(p.name) as partner,
    rtrim(m.name) as metric,
    mt.target::int as value
from {{source('directus', 'metric_targets')}} mt
left join {{source('directus', 'metrics')}} m on m.id = mt.metric_id
left join {{source('directus', 'partners_countries')}} pc on pc.id = mt.partner_country_id
left join {{source('directus', 'partners')}} p on p.id = pc.partners_id
left join {{source('directus', 'countries')}} c on c.id = pc.countries_id
where m.name = 'expected_session_time'