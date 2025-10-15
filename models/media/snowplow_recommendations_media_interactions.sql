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
  coalesce(cast(user_id as string), user_identifier) as user_id,
  media_identifier as item_id,
  {{ snowplow_utils.to_unixtstamp("start_tstamp") }} as timestamp,
  'Watch' as event_type

from {{ var("media_player_source") }} as e

  where (date(start_tstamp) >= {%- if var('media_start_date') == '' %}
                                 current_date() - '{{ var('media_look_back_days') }}'
                                {%- else %}
                                '{{ var('media_start_date') }}'
                                {%- endif %}
    and date(start_tstamp) <= {%- if var('media_end_date') == '' %}
                                 current_date() - 1
                                {%- else %}
                                '{{ var('media_end_date') }}'
                                {%- endif %})
    and is_valid_play = True
