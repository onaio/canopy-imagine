select 
id::int,
rtrim(name) as name,
rtrim(role) as role,
rtrim(gender) as gender,
supervisor_id,
rtrim(country) as country,
rtrim(comments) as comments,
start_date::date,
end_date::date,
partner_id
from {{source('airbyte','staff')}}