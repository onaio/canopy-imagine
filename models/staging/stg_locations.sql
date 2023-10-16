select 
    l.id::int, 
    latitude::float as lat,
    longitude::float as lon,
    rtrim(l.name) as name,
    rtrim(type) as type,
    rtrim(c.name) as country,
    staff_id::int,
    rtrim(administrative_area) as admin_3_name
from {{source('directus', 'locations')}} l
left join {{source('directus', 'countries')}} c on c.id = l.country_id