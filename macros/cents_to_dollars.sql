{%- macro cents_to_dollars(columnname, decimal_places =2 ) -%}
round(1.0* {{columnname}} / 100, {{decimal_places}} )
{%- endmacro -%}