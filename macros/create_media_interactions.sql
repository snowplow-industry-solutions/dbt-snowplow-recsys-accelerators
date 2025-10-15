{#
Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}

{% macro create_media_interactions() %}
  {{ return(adapter.dispatch('create_media_interactions', 'snowplow_recommendations')()) }}
{% endmacro %}

{% macro default__create_media_interactions() %}
  {% if target.type in ('snowflake', 'databricks') %}
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
  {% else %}
    {% do exceptions.raise_compiler_error('Macro create_media_interactions is only for Snowflake or Databricks, it is not supported for ' ~ target.type) %}
  {% endif %}
{% endmacro %}
