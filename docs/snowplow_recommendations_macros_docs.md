{% docs macro_create_ecommerce_interactions %}
This macro is used to build the sql query to create the model for the E-commerce Interactions dataset.
To allow for the customization of each user's different metadata they'd like to have in this dataset/model we recommend writing
your dataset model in this macro.

#### Returns

The sql to create the model for E-commerce's Interactions dataset.
{% enddocs %}

{% docs macro_create_ecommerce_items %}
This macro is used to build the sql query to create the model for the E-commerce Items dataset.
To allow for the customization of each user's different metadata they'd like to have in this dataset/model we recommend writing
your dataset model in this macro.

#### Returns

The sql to create the model for E-commerce's Items dataset.
{% enddocs %}

{% docs macro_create_ecommerce_users %}
This macro is used to build the sql query to create the model for the E-commerce Users dataset.
To allow for the customization of each user's different metadata they'd like to have in this dataset/model we recommend writing
your dataset model in this macro.

#### Returns

The sql to create the model for E-commerce's Users dataset.
{% enddocs %}

{% docs macro_create_media_interactions %}
This macro is used to build the sql query to create the model for the Media Interactions dataset.
To allow for the customization of each user's different metadata they'd like to have in this dataset/model we recommend writing
your dataset model in this macro.

#### Returns

The sql to create the model for Media's Interactions dataset.
{% enddocs %}

{% docs macro_export_data_to_s3 %}
This macro is used to export the tables listed in the var `tables_to_export` after the dbt package finishes running.
This macro dispatches to adapter implementations, only a Snowflake implementation is provided.
The Snowflake implementation depends on the `snowflake_stage_name` var.
{% enddocs %}

{% docs macro_external_table_config %}
This macro checks the current model against the `tables_to_export` var, and if it matches, updates the current model's config to export to the external catalog in the `external_catalog` var.
This is only implemented if the target warehouse is Databricks, and creates external tables on AWS S3.
The tables will then be unloaded as csv files to the S3 bucket configured via the var `s3_bucket`.
{% enddocs %}
