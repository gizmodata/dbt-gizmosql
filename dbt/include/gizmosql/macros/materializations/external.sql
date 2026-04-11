{#
  The `external` materialization writes a model's results to files on the
  GizmoSQL server's filesystem (or any cloud-storage URI the server's DuckDB
  backend can reach — s3://, gs://, azure://, ...).

  All I/O runs server-side: the COPY statement executes on the GizmoSQL server,
  which is typically a powerful cloud VM with many CPUs, lots of memory, fast
  disk, and a fat NIC. That's the point — push the bulk write to the box that
  has the resources instead of pulling the result set back to the client.

  After writing, a view is created on top of the file via read_parquet /
  read_csv / read_json so downstream dbt models can ref() it like any other
  relation.

  Adapted from dbt-duckdb's external materialization.
#}
{% materialization external, adapter="gizmosql", supported_languages=['sql', 'python'] %}

  {#- fail fast on unsupported plugin/glue knobs before doing any work -#}
  {%- set plugin_name = config.get('plugin') -%}
  {%- set glue_register = config.get('glue_register', default=false) -%}
  {%- if plugin_name is not none or glue_register is true -%}
    {%- set _unsupported = plugin_name if plugin_name is not none else 'glue' -%}
    {{ exceptions.raise_compiler_error(
        "The '" ~ _unsupported ~ "' plugin is not supported by dbt-gizmosql. "
        "dbt-duckdb plugins (including 'glue') run client-side and have no "
        "analogue in the server-side gizmosql adapter."
    ) }}
  {%- endif -%}

  {%- set location = render(config.get('location', default=external_location(this, config))) -%}
  {%- set rendered_options = render_write_options(config) -%}

  {%- set format = config.get('format') -%}
  {%- set allowed_formats = ['csv', 'parquet', 'json'] -%}
  {%- if format -%}
      {%- if format not in allowed_formats -%}
          {{ exceptions.raise_compiler_error("Invalid format: " ~ format ~ ". Allowed formats are: " ~ allowed_formats | join(', ')) }}
      {%- endif -%}
  {%- else -%}
    {%- set format = location.split('.')[-1].lower() if '.' in location else 'parquet' -%}
    {%- set format = format if format in allowed_formats else 'parquet' -%}
  {%- endif -%}

  {%- set write_options = adapter.external_write_options(location, rendered_options) -%}
  {%- set read_location = adapter.external_read_location(location, rendered_options) -%}
  {%- set parquet_read_options = config.get('parquet_read_options', {'union_by_name': False}) -%}
  {%- set json_read_options = config.get('json_read_options', {'auto_detect': True}) -%}
  {%- set csv_read_options = config.get('csv_read_options', {'auto_detect': True}) -%}

  {%- set language = model['language'] -%}
  {%- set target_relation = this.incorporate(type='view') %}

  {%- set existing_relation = load_cached_relation(this) -%}
  {%- set temp_relation = make_intermediate_relation(this.incorporate(type='table'), suffix='__dbt_tmp') -%}
  {%- set intermediate_relation = make_intermediate_relation(target_relation, suffix='__dbt_int') -%}
  {%- set preexisting_temp_relation = load_cached_relation(temp_relation) -%}
  {%- set preexisting_intermediate_relation = load_cached_relation(intermediate_relation) -%}
  {%- set backup_relation_type = 'table' if existing_relation is none else existing_relation.type -%}
  {%- set backup_relation = make_backup_relation(target_relation, backup_relation_type) -%}
  {%- set preexisting_backup_relation = load_cached_relation(backup_relation) -%}
  {% set grant_config = config.get('grants') %}

  {{ drop_relation_if_exists(preexisting_intermediate_relation) }}
  {{ drop_relation_if_exists(preexisting_temp_relation) }}
  {{ drop_relation_if_exists(preexisting_backup_relation) }}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  -- build a staging table holding the model's result set
  {% call statement('create_table', language=language) -%}
    {{- create_table_as(False, temp_relation, compiled_code, language) }}
  {%- endcall %}

  -- count rows so we can handle the empty-result case below
  {%- set count_query -%}
    select count(*) as row_count from {{ temp_relation }}
  {%- endset -%}
  {%- set row_count = run_query(count_query) -%}

  -- pad an all-null row when empty so the file retains a readable schema;
  -- we filter it back out in the read-side view below
  {% call statement('main', language='sql') -%}
    {% if row_count[0][0] == 0 %}
    insert into {{ temp_relation }} values (
      {%- for col in get_columns_in_relation(temp_relation) -%}
      NULL{{ "," if not loop.last }}
      {%- endfor -%}
    )
    {% else %}
    select 1
    {% endif %}
  {%- endcall %}

  -- server-side COPY: the GizmoSQL server writes directly to `location`
  {{ write_to_file(temp_relation, location, write_options) }}

  -- create a view over the file so downstream models can `ref()` this one
  {% call statement('create_view', language='sql') -%}
    {% if format == 'json' %}
      create or replace view {{ intermediate_relation }} as (
        select * from read_json('{{ read_location }}'
        {%- for key, value in json_read_options.items() -%}
          , {{ key }}=
          {%- if value is string -%}
            '{{ value }}'
          {%- else -%}
            {{ value }}
          {%- endif -%}
        {%- endfor -%}
        )
        {% if row_count[0][0] == 0 %}
          where 1
          {%- for col in get_columns_in_relation(temp_relation) -%}
            {{ '' }} AND "{{ col.column }}" is not NULL
          {%- endfor -%}
        {% endif %}
      );
    {% elif format == 'parquet' %}
      create or replace view {{ intermediate_relation }} as (
        select * from read_parquet('{{ read_location }}'
        {%- for key, value in parquet_read_options.items() -%}
          , {{ key }}=
          {%- if value is string -%}
            '{{ value }}'
          {%- else -%}
            {{ value }}
          {%- endif -%}
        {%- endfor -%}
        )
        {% if row_count[0][0] == 0 %}
          where 1
          {%- for col in get_columns_in_relation(temp_relation) -%}
            {{ '' }} AND "{{ col.column }}" is not NULL
          {%- endfor -%}
        {% endif %}
      );
    {% elif format == 'csv' %}
      create or replace view {{ intermediate_relation }} as (
        select * from read_csv('{{ read_location }}'
        {%- for key, value in csv_read_options.items() -%}
          , {{ key }}=
          {%- if value is string -%}
            '{{ value }}'
          {%- else -%}
            {{ value }}
          {%- endif -%}
        {%- endfor -%}
        )
        {% if row_count[0][0] == 0 %}
          where 1
          {%- for col in get_columns_in_relation(temp_relation) -%}
            {{ '' }} AND "{{ col.column }}" is not NULL
          {%- endfor -%}
        {% endif %}
      );
    {% endif %}
  {%- endcall %}

  -- swap the new view into place
  {% if existing_relation is not none %}
      {{ adapter.rename_relation(existing_relation, backup_relation) }}
  {% endif %}

  {{ adapter.rename_relation(intermediate_relation, target_relation) }}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  {% set should_revoke = should_revoke(existing_relation, full_refresh_mode=True) %}
  {% do apply_grants(target_relation, grant_config, should_revoke=should_revoke) %}

  {% do persist_docs(target_relation, model) %}

  {{ adapter.commit() }}

  -- drop the backup view and the staging table
  {{ drop_relation_if_exists(backup_relation) }}
  {{ drop_relation_if_exists(temp_relation) }}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}
