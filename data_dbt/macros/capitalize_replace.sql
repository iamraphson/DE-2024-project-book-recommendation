
{% macro capitalize_replace(column_name) %}
    {% if target.type == 'bigquery' %}
        replace(INITCAP({{ column_name }}), '"', '')
    {% elif target.type == 'postgres' %}
        replace(INITCAP({{ column_name }}), '"', '')
    {% elif target.type == 'snowflake' %}
        replace(INITCAP({{ column_name }}), '"', '')
    {% else %}
        replace(UPPER(LEFT({{ column_name }}, 1)) || LOWER(SUBSTRING({{ column_name }}, 2)), '"', '')
    {% endif %}
{% endmacro %}
