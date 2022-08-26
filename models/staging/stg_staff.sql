select 
id::int,
name,
role,
gender,
supervisor_id,
country,
comments,
start_date::date,
end_date::date,
partner_id
from {{source('airbyte','staff')}}