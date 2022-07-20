{%- macro limit_data_in_dev(time_field, n_days = 3 ) -%}
{%- if target.name != 'prod' -%}
where {{time_field}} >=  current_timestamp - interval '{{n_days}}' day
{%- endif -%}
{%- endmacro -%}