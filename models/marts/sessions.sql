{{
    config(
        materialized='incremental',
        unique_key='id'
    )
}}

WITH main_sessions AS (
	select 
		session_unique_id as id,
		dtd.country as tablet_country,
		ts.device_id,
        dtd.serial_number,
		start_time::timestamp ,
		end_time::timestamp ,
		duration,
		dtd.location_id,
		dtd.location,
		dtd.date_launched,
		dtd.admin_3_name,
		dtd.country,
        dtd.partner,
		dtd.field_officer,
		dtd.week,
		dtd.term_id ,
		dtd.term_name,
		dtd.week_number as term_week,
		usb,
		eidu,
        ts.processed_at
	from {{ref('stg_unique_sessions')}} ts 
    left join {{ ref('stg_devices_per_week') }} dtd 
    on 
    (ts.device_id = dtd.device_id ) 
       and date_trunc('week', ts.start_time::timestamp)::date = dtd.week
)

select
	ms.id,
	ms.device_id,
    ms.serial_number,
	ms.tablet_country,
	ms.start_time::timestamp,
	ms.end_time::timestamp,
	ms.duration,
	ms.location_id,
	ms.location,
	ms.date_launched,
	ms.admin_3_name,
	ms.country,
    ms.partner,
	ms.field_officer,
	ms.week,
	ms.term_id,
	ms.term_name,
	ms.term_week,
	usb,
	eidu,
    ms.processed_at, 
	NULL as is_last_week, --AP. 2023.10.27 AP: aiming to simplify. Original: case when date_trunc('week', start_time::timestamp)::date = rd.most_recent_date then 'Yes' else 'No' end as is_last_week,
	NULL as is_latest_term --AP. 2023.10.27 AP: aiming to simplify. Original: case when rt.latest_term = true and ms.term_id = rt.id then 'Yes' else 'No' end as is_latest_term
from main_sessions ms

{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  where processed_at > (select max(processed_at) from {{ this }})
{% endif %}
