select 
s.id::int,
rtrim(concat(first_name, last_name)) as name,
rtrim(sr.name) as role,
supervisor_id,
rtrim(c.name) as country,
start_date::date,
end_date::date,
pc.partners_id as partner_id
from {{source('directus','staff')}} s
left join {{source('directus', 'staff_roles')}} sr on sr.id = s.staff_role_id
left join {{source('directus', 'partners_countries')}} pc on pc.id = s.partner_country_id
left join {{source ('directus', 'countries')}} c on c.id = pc.countries_id