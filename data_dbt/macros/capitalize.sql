{% macro capitalize(column_name) %}
    {% if target.database.type == 'bigquery' %}
        INITCAP({{ column_name }}) AS {{ column_name }}
    {% elif target.database.type == 'postgres' %}
        INITCAP({{ column_name }}) AS {{ column_name }}
    {% elif target.database.type == 'snowflake' %}
        INITCAP({{ column_name }}) AS {{ column_name }}
    {% else %}
        UPPER(LEFT({{ column_name }}, 1)) || LOWER(SUBSTRING({{ column_name }}, 2)) AS {{ column_name }}
    {% endif %}
{% endmacro %}
