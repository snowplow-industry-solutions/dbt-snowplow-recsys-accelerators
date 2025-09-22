{#
Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}

{{
  config(
    sql_header=snowplow_utils.set_query_tag(var('snowplow__query_tag', 'snowplow_dbt'))
  )
}}

{# Configs for Databricks to create an external table with csv files on an external location #}
{{ external_table_config(this.name) }}


select
  coalesce(ecommerce_user_id, domain_userid) as user_id,
  product_id as item_id,
  {{ snowplow_utils.to_unixtstamp("derived_tstamp") }} as timestamp,
 
  case
    when is_product_view then 'View' else 'Purchase'
  end as event_type

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
