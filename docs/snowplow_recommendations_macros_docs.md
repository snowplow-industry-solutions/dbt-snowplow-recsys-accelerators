{% docs macro_create_ecommerce_items %}
{% raw %}		
This macro is used to build the sql query to create the model for the E-commerce Items dataset.
To allow for the customization of each user's different metadata they'd like to have in this dataset/model we recommend writing
your dataset model in this macro.

#### Returns

The sql to create the model for E-commerce's Items dataset.
{% endraw %}
{% enddocs %}

{% docs macro_create_ecommerce_users %}
{% raw %}		
This macro is used to build the sql query to create the model for the E-commerce Users dataset.
To allow for the customization of each user's different metadata they'd like to have in this dataset/model we recommend writing
your dataset model in this macro.

#### Returns

The sql to create the model for E-commerce's Users dataset.
{% endraw %}
{% enddocs %}

{% docs macro_export_data_to_s3 %}
{% raw %}		
This macro is used to export the tables listed in the var `tables_to_export` when the dbt package is running on a Snowflake warehouse.
{% endraw %}
{% enddocs %}

{% docs macro_external_table_config %}
{% raw %}		
When the target warehouse is Databricks then this macro will dump the required configurations to your model(s) to allow them to be
created as external tables on AWS S3. The tables will then be unloaded as csv files to the S3 bucket configured via the var `s3_bucket`.
{% endraw %}
{% enddocs %}