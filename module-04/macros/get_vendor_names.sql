{% macro get_vendor_names (vendor_id) -%}
CASE
    WHEN {{ vendor_id}} = 1 THEN 'Mobil Tech LLC'
    WHEN {{ vendor_id}} = 2 THEN 'Local Tech LLC'
    WHEN {{ vendor_id}} = 4 THEN 'Unknown Vendor'
END
{%- endmacro %}