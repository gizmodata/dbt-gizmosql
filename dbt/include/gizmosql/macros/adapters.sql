{% macro gizmosql__create_schema(relation) -%}
  {%- call statement('create_schema') -%}
    {% set sql %}
        select type from duckdb_databases()
        where lower(database_name)='{{ relation.database | lower }}'
        and type='sqlite'
    {% endset %}
    {% set results = run_query(sql) %}
    {% if results|length == 0 %}
        create schema if not exists {{ relation.without_identifier() }}
    {% else %}
        {% if relation.schema!='main' %}
            {{ exceptions.raise_compiler_error(
                "Schema must be 'main' when writing to sqlite "
                ~ "instead got " ~ relation.schema
            )}}
        {% endif %}
    {% endif %}
  {%- endcall -%}
{% endmacro %}

{% macro gizmosql__drop_schema(relation) -%}
  {%- call statement('drop_schema') -%}
    drop schema if exists {{ relation.without_identifier() }} cascade
  {%- endcall -%}
{% endmacro %}

{% macro gizmosql__list_schemas(database) -%}
  {% set sql %}
    select schema_name
    from system.information_schema.schemata
    {% if database is not none %}
    where lower(catalog_name) = '{{ database | lower | replace('"', '') }}'
    {% endif %}
  {% endset %}
  {{ return(run_query(sql)) }}
{% endmacro %}

{% macro gizmosql__check_schema_exists(information_schema, schema) -%}
  {% set sql -%}
        select count(*)
        from system.information_schema.schemata
        where lower(schema_name) = '{{ schema | lower }}'
        and lower(catalog_name) = '{{ information_schema.database | lower }}'
  {%- endset %}
  {{ return(run_query(sql)) }}
{% endmacro %}

{% macro get_column_names() %}
  {# loop through user_provided_columns to get column names #}
    {%- set user_provided_columns = model['columns'] -%}
    (
    {% for i in user_provided_columns %}
      {% set col = user_provided_columns[i] %}
      {{ col['name'] }} {{ "," if not loop.last }}
    {% endfor %}
  )
{% endmacro %}


{% macro gizmosql__create_table_as(temporary, relation, compiled_code, language='sql') -%}
  {%- if language == 'sql' -%}
    {% set contract_config = config.get('contract') %}
    {% if contract_config.enforced %}
      {{ get_assert_columns_equivalent(compiled_code) }}
    {% endif %}
    {%- set sql_header = config.get('sql_header', none) -%}

    {{ sql_header if sql_header is not none }}

    create {% if temporary: -%}temporary{%- endif %} table
      {{ relation.include(database=(not temporary), schema=(not temporary)) }}
  {% if contract_config.enforced and not temporary %}
    {#-- DuckDB doesnt support constraints on temp tables --#}
    {{ get_table_columns_and_constraints() }} ;
    insert into {{ relation }} {{ get_column_names() }} (
      {{ get_select_subquery(compiled_code) }}
    );
  {% else %}
    as (
      {{ compiled_code }}
    );
  {% endif %}
  {%- elif language == 'python' -%}
    {{ py_write_table(temporary=temporary, relation=relation, compiled_code=compiled_code) }}
  {%- else -%}
      {% do exceptions.raise_compiler_error("gizmosql__create_table_as macro didn't get supported language, it got %s" % language) %}
  {%- endif -%}
{% endmacro %}

{% macro py_write_table(temporary, relation, compiled_code) -%}
{{ compiled_code }}

def materialize(df, con):
    try:
        import pyarrow
        pyarrow_available = True
    except ImportError:
        pyarrow_available = False
    finally:
        if pyarrow_available and isinstance(df, pyarrow.Table):
            # https://github.com/duckdb/duckdb/issues/6584
            import pyarrow.dataset
    tmp_name = '__dbt_python_model_df_' + '{{ relation.identifier }}'
    con.register(tmp_name, df)
    con.execute('create table {{ relation }} as select * from ' + tmp_name)
    con.unregister(tmp_name)
{% endmacro %}

{% macro gizmosql__create_view_as(relation, sql) -%}
  {% set contract_config = config.get('contract') %}
  {% if contract_config.enforced %}
    {{ get_assert_columns_equivalent(sql) }}
  {%- endif %}
  {%- set sql_header = config.get('sql_header', none) -%}

  {{ sql_header if sql_header is not none }}
  create view {{ relation }} as (
    {{ sql }}
  );
{% endmacro %}

{% macro gizmosql__get_columns_in_relation(relation) -%}
  {% call statement('get_columns_in_relation', fetch_result=True) %}
      select
          column_name,
          data_type,
          character_maximum_length,
          numeric_precision,
          numeric_scale

      from system.information_schema.columns
      where table_name = '{{ relation.identifier }}'
      {% if relation.schema %}
      and lower(table_schema) = '{{ relation.schema | lower }}'
      {% endif %}
      {% if relation.database %}
      and lower(table_catalog) = '{{ relation.database | lower }}'
      {% endif %}
      order by ordinal_position

  {% endcall %}
  {% set table = load_result('get_columns_in_relation').table %}
  {{ return(sql_convert_columns_in_relation(table)) }}
{% endmacro %}

{% macro gizmosql__list_relations_without_caching(schema_relation) %}
  {% call statement('list_relations_without_caching', fetch_result=True) -%}
    select
      '{{ schema_relation.database }}' as database,
      table_name as name,
      table_schema as schema,
      CASE table_type
        WHEN 'BASE TABLE' THEN 'table'
        WHEN 'VIEW' THEN 'view'
        WHEN 'LOCAL TEMPORARY' THEN 'table'
        END as type
    from system.information_schema.tables
    where lower(table_schema) = '{{ schema_relation.schema | lower }}'
    and lower(table_catalog) = '{{ schema_relation.database | lower }}'
  {% endcall %}
  {{ return(load_result('list_relations_without_caching').table) }}
{% endmacro %}

{% macro gizmosql__drop_relation(relation) -%}
  {% call statement('drop_relation', auto_begin=False) -%}
    drop {{ relation.type }} if exists {{ relation }} cascade
  {%- endcall %}
{% endmacro %}

{% macro gizmosql__rename_relation(from_relation, to_relation) -%}
  {% set target_name = adapter.quote_as_configured(to_relation.identifier, 'identifier') %}
  {% call statement('rename_relation') -%}
    alter {{ to_relation.type }} {{ from_relation }} rename to {{ target_name }}
  {%- endcall %}
{% endmacro %}

{% macro gizmosql__make_temp_relation(base_relation, suffix) %}
    {% set tmp_identifier = base_relation.identifier ~ suffix ~ py_current_timestring() %}
    {% do return(base_relation.incorporate(
                                  path={
                                    "identifier": tmp_identifier,
                                    "schema": none,
                                    "database": none
                                  })) -%}
{% endmacro %}

{% macro gizmosql__current_timestamp() -%}
  now()
{%- endmacro %}

{% macro gizmosql__snapshot_string_as_time(timestamp) -%}
    {%- set result = "'" ~ timestamp ~ "'::timestamp" -%}
    {{ return(result) }}
{%- endmacro %}

{% macro gizmosql__snapshot_get_time() -%}
  {{ current_timestamp() }}::timestamp
{%- endmacro %}

{% macro gizmosql__get_incremental_default_sql(arg_dict) %}
  {% do return(get_incremental_delete_insert_sql(arg_dict)) %}
{% endmacro %}

{% macro location_exists(location) -%}
  {% do return(adapter.location_exists(location)) %}
{% endmacro %}

{% macro write_to_file(relation, location, options) -%}
  {% call statement('write_to_file') -%}
    copy {{ relation }} to '{{ location }}' ({{ options }})
  {%- endcall %}
{% endmacro %}

{% macro store_relation(plugin, relation, location, format, config) -%}
  {%- set column_list = adapter.get_columns_in_relation(relation) -%}
  {% do adapter.store_relation(plugin, relation, column_list, location, format, config) %}
{% endmacro %}

{% macro render_write_options(config) -%}
  {% set options = config.get('options', {}) %}
  {% if options is not mapping %}
    {% do exceptions.raise_compiler_error("The options argument must be a dictionary") %}
  {% endif %}

  {% for k in options %}
    {% set _ = options.update({k: render(options[k])}) %}
  {% endfor %}

  {# legacy top-level write options #}
  {% if config.get('format') %}
    {% set _ = options.update({'format': render(config.get('format'))}) %}
  {% endif %}
  {% if config.get('delimiter') %}
    {% set _ = options.update({'delimiter': render(config.get('delimiter'))}) %}
  {% endif %}

  {% do return(options) %}
{%- endmacro %}

{% macro gizmosql__apply_grants(relation, grant_config, should_revoke=True) %}
    {#-- If grant_config is {} or None, this is a no-op --#}
    {% if grant_config %}
      {{ adapter.warn_once('Grants for relations are not supported by GizmoSQL') }}
    {% endif %}
{% endmacro %}

{% macro gizmosql__get_create_index_sql(relation, index_dict) -%}
  {%- set index_config = adapter.parse_index(index_dict) -%}
  {%- set comma_separated_columns = ", ".join(index_config.columns) -%}
  {%- set index_name = index_config.render(relation) -%}

  create {% if index_config.unique -%}
    unique
  {%- endif %} index
  "{{ index_name }}"
  on {{ relation }}
  ({{ comma_separated_columns }});
{%- endmacro %}

{% macro drop_indexes_on_relation(relation) -%}
  {% call statement('get_indexes_on_relation', fetch_result=True) %}
    SELECT index_name
    FROM duckdb_indexes()
    WHERE schema_name = '{{ relation.schema }}'
      AND table_name = '{{ relation.identifier }}'
  {% endcall %}

  {% set results = load_result('get_indexes_on_relation').table %}
  {% for row in results %}
    {% set index_name = row[0] %}
    {% call statement('drop_index_' + loop.index|string, auto_begin=false) %}
      DROP INDEX "{{ relation.schema }}"."{{ index_name }}"
    {% endcall %}
  {% endfor %}

  {#-- Verify indexes were dropped --#}
  {% call statement('verify_indexes_dropped', fetch_result=True) %}
    SELECT COUNT(*) as remaining_indexes
    FROM duckdb_indexes()
    WHERE schema_name = '{{ relation.schema }}'
      AND table_name = '{{ relation.identifier }}'
  {% endcall %}
  {% set verify_results = load_result('verify_indexes_dropped').table %}
{%- endmacro %}

{% macro gizmosql__get_binding_char() %}
  {{ return('?') }}
{% endmacro %}

{% macro gizmosql__get_merge_sql(target, source, unique_key, dest_columns, incremental_predicates=none) -%}
    {%- set predicates = [] if incremental_predicates is none else [] + incremental_predicates -%}
    {%- set dest_cols_csv = get_quoted_csv(dest_columns | map(attribute="name")) -%}

    {%- set source_aliased_insert_cols = [] -%}
    {%- for col in source_columns -%}
        {%- do source_aliased_insert_cols.append("DBT_INTERNAL_SOURCE." ~ col) -%}
    {%- endfor -%}

    {%- set source_aliased_insert_cols_csv = source_aliased_insert_cols | join(', ') -%}

    {%- set merge_update_columns = config.get('merge_update_columns') -%}
    {%- set merge_exclude_columns = config.get('merge_exclude_columns') -%}
    {%- set update_columns = get_merge_update_columns(merge_update_columns, merge_exclude_columns, dest_columns) -%}
    {%- set sql_header = config.get('sql_header', none) -%}

    {% if unique_key %}
        {% if unique_key is sequence and unique_key is not mapping and unique_key is not string %}
            {% for key in unique_key %}
                {% set this_key_match %}
                    DBT_INTERNAL_SOURCE.{{ key }} = DBT_INTERNAL_DEST.{{ key }}
                {% endset %}
                {% do predicates.append(this_key_match) %}
            {% endfor %}
        {% else %}
            {% set source_unique_key = ("DBT_INTERNAL_SOURCE." ~ unique_key) | trim %}
	    {% set target_unique_key = ("DBT_INTERNAL_DEST." ~ unique_key) | trim %}
	    {% set unique_key_match = equals(source_unique_key, target_unique_key) | trim %}
            {% do predicates.append(unique_key_match) %}
        {% endif %}
    {% else %}
        {% do predicates.append('FALSE') %}
    {% endif %}

    {{ sql_header if sql_header is not none }}

    merge into {{ target }} as DBT_INTERNAL_DEST
        using {{ source }} as DBT_INTERNAL_SOURCE
        on {{"(" ~ predicates | join(") and (") ~ ")"}}

    {% if unique_key %}
    when matched then update set
        {% for column_name in update_columns -%}
            {{ column_name }} = DBT_INTERNAL_SOURCE.{{ column_name }}
            {%- if not loop.last %}, {%- endif %}
        {%- endfor %}
    {% endif %}

    when not matched then insert
        ({{ dest_cols_csv }})
    values
        ({{ source_aliased_insert_cols_csv }})

{% endmacro %}

{% macro gizmosql__snapshot_merge_sql(target, source, insert_cols) -%}
    {%- set insert_cols_csv = insert_cols | join(', ') -%}
    {%- set source_aliased_insert_cols = [] -%}
    {%- for col in insert_cols -%}
        {%- do source_aliased_insert_cols.append("DBT_INTERNAL_SOURCE." ~ col) -%}
    {%- endfor -%}

    {%- set source_aliased_insert_cols_csv = source_aliased_insert_cols | join(', ') -%}

    {%- set columns = config.get("snapshot_table_column_names") or get_snapshot_table_column_names() -%}

    merge into {{ target.render() }} as DBT_INTERNAL_DEST
    using {{ source }} as DBT_INTERNAL_SOURCE
    on DBT_INTERNAL_SOURCE.{{ columns.dbt_scd_id }} = DBT_INTERNAL_DEST.{{ columns.dbt_scd_id }}

    when matched
     {% if config.get("dbt_valid_to_current") %}
	{% set source_unique_key = ("DBT_INTERNAL_DEST." ~ columns.dbt_valid_to) | trim %}
	{% set target_unique_key = config.get('dbt_valid_to_current') | trim %}
	and ({{ equals(source_unique_key, target_unique_key) }} or {{ source_unique_key }} is null)

     {% else %}
       and DBT_INTERNAL_DEST.{{ columns.dbt_valid_to }} is null
     {% endif %}
     and DBT_INTERNAL_SOURCE.dbt_change_type in ('update', 'delete')
        then update
        set {{ columns.dbt_valid_to }} = DBT_INTERNAL_SOURCE.{{ columns.dbt_valid_to }}

    when not matched
     and DBT_INTERNAL_SOURCE.dbt_change_type = 'insert'
        then insert ({{ insert_cols_csv }})
        values ({{ source_aliased_insert_cols_csv }})

{% endmacro %}