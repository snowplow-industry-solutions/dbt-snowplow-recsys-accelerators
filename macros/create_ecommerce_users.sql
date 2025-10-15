{#
Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}

{% macro create_ecommerce_users() %}
  {{ return(adapter.dispatch('create_ecommerce_users', 'snowplow_recommendations')()) }}
{% endmacro %}

{% macro default__create_ecommerce_users() %}
  {% if target.type in ('snowflake', 'databricks') %}
    select distinct
      coalesce(ecommerce_user_id, domain_userid) as user_id,
      coalesce(ecommerce_user_is_guest, ecommerce_user_id is null) ecommerce_user_is_guest,

    from {{ var('ecommerce_product_source' ) }}
      where (is_product_view or is_product_transaction)

      and (date(derived_tstamp) >= {%- if var('ecommerce_start_date') == '' %}
                                    current_date() - '{{ var('ecommerce_look_back_days') }}'
                                    {%- else %}
                                    '{{ var('ecommerce_start_date') }}'
                                    {%- endif %}
        and date(derived_tstamp) <= {%- if var('ecommerce_end_date') == '' %}
                                    current_date() - 1
                                    {%- else %}
                                    '{{ var('ecommerce_end_date') }}'
                                    {%- endif %})
  {% else %}
    {% do exceptions.raise_compiler_error('Macro create_ecommerce_users is only for Snowflake or Databricks, it is not supported for ' ~ target.type) %}
  {% endif %}
{% endmacro %}
