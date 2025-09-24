{#
Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}

{% macro export_to_s3() %}
  {% if execute %}
    {% set tables_to_export = var('tables_to_export') %}
    {% for res in results -%}
      {% if res.node.name in tables_to_export and res.status != 'error' %}
        {{ adapter.dispatch('export_to_s3', 'snowplow_recommendations')(res.node.name) }}
      {% endif %}
    {% endfor %}
  {% endif %}
{% endmacro %}

{% macro default__export_to_s3() %}
{% endmacro %}

{% macro snowflake__export_to_s3(table) %}
  {% set stage_name = var('snowflake_stage_name') %}
  {% set export_query %}
    COPY INTO @{{stage_name}}/{{table}}/personalize-dataset.csv from {{schema}}.{{table}}
    FILE_FORMAT = (TYPE = CSV COMPRESSION = NONE ESCAPE = '0x5c' FIELD_OPTIONALLY_ENCLOSED_BY = '"' )
    SINGLE = TRUE
    OVERWRITE = TRUE
    HEADER = TRUE;

    COPY INTO @{{stage_name}}/{{table}}/personalize-dataset.parquet from {{schema}}.{{table}}
    FILE_FORMAT = (TYPE = PARQUET COMPRESSION = NONE ESCAPE = '0x5c' FIELD_OPTIONALLY_ENCLOSED_BY = '"' )
    SINGLE = TRUE
    OVERWRITE = TRUE
    HEADER = TRUE;
  {% endset %}
  {% do run_query(export_query) %}
{% endmacro %}
