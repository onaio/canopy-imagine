select 
{{ cents_to_dollars('duration', 4) }} as duration2,  --note that the quotation marks indicate a string
* from 
{{ref('sessions')}}
{{ limit_data_in_dev('start_time', 1000)}}