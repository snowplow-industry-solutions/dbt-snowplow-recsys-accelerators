{#
Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}

{% macro external_table_config(table_name) %}
  {{ return(adapter.dispatch('external_table_config', 'snowplow_recommendations')(table_name)) }}
{% endmacro %}

{% macro default__external_table_config(table_name) %}
{% endmacro %}

{% macro databricks__external_table_config(table_name) %}
{% if table_name in var('tables_to_export') %}
  {{
    config(
      database=var('external_catalog'),
      location_root=var('s3_bucket'),
      file_format='CSV',
      options={'header': 'true'}
    )
  }}
{% endif %}
{% endmacro %}
