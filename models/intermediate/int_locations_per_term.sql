--- model with each row for one location in one term. Includes basic metrics such as enrolment per location

with location_total_enrollment as (
select
	term_id,
	location_id,
	SUM(number::int) as children
from {{ref('stg_location_enrollments')}}
group by 1,2
)

select 
	da.location_id,
    l."name" as location,
 	da.children,
	da.term_id,
    t.name as term_name,
	t.start_date as term_start,
	t.end_date as term_end, 
    l.admin_3_name as admin_3_name,
    l.country,
    p.name as partner,
    s.name as field_officer
from location_total_enrollment  da
left join {{ref('stg_terms')}} t on da.term_id = t.id
left join {{ref('stg_locations')}} l on da.location_id = l.id 
left join {{ref('stg_staff')}} s on s.id = l.staff_id
left join {{ref('stg_partners') }} p on s.partner_id::int = p.id	