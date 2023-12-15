-- identify complete sessions, i.e. sessions >13 min 
with complete_session as (
    select *
    from {{ ref('sessions') }}
    where  duration >= 13*60 and location is not null and country = 'Malawi'
),
-- total complete sessions per day per facility
complete_sessions_daily as 
(select start_time::date , location, admin_3_name, country, partner, week , count(*) as complete_sessions
from complete_session
group by 1,2,3,4,5,6
order by 1 desc
),
-- students_with_complete_sessions weekly. The assumption is that kids use tablets every day, so the day with the max number 
-- of kids is the day with most unique students benefiting from the tablet. The methodology comes from the client
students_using_tablets_weekly as 
(select week, location, admin_3_name, country, partner, max(complete_sessions) as students_using_tablets
from complete_sessions_daily 
group by 1,2,3,4,5 
order by location, week desc
)
select 
ws.* , st.students_using_tablets 
from {{ ref('malawi_location_weekly_sessions') }} ws 
left join students_using_tablets_weekly st on ws.week = st.week and ws.location=st.location and ws.admin_3_name = st.admin_3_name
