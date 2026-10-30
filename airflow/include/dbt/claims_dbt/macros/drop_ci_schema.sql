{#
  Drops the throwaway schema a CI run built into. Used by the GitHub Actions workflow:
      dbt run-operation drop_ci_schema --target ci
  Two guards make it impossible to drop ANALYTICS or RAW by mistake:
  it only runs on the ci target, and only on a schema whose name starts with CI_.
#}
{% macro drop_ci_schema() %}
  {% if target.name != 'ci' %}
    {{ exceptions.raise_compiler_error("drop_ci_schema only runs on the ci target, not '" ~ target.name ~ "'") }}
  {% endif %}
  {% if not target.schema.upper().startswith('CI_') %}
    {{ exceptions.raise_compiler_error("Refusing to drop schema '" ~ target.schema ~ "': name must start with CI_") }}
  {% endif %}
  {% set sql %}drop schema if exists {{ target.database }}.{{ target.schema }} cascade{% endset %}
  {% do run_query(sql) %}
  {{ log("Dropped schema " ~ target.database ~ "." ~ target.schema, info=True) }}
{% endmacro %}
