{% materialization table, adapter='gizmosql', supported_languages=['sql', 'python'] %}

  {%- set language = model['language'] -%}
  {%- set existing_relation = load_cached_relation(this) -%}
  {%- set target_relation = this.incorporate(type='table') %}
  {%- set intermediate_relation = make_intermediate_relation(target_relation) -%}
  {%- set preexisting_intermediate_relation = load_cached_relation(intermediate_relation) -%}
  {%- set backup_relation_type = 'table' if existing_relation is none else existing_relation.type -%}
  {%- set backup_relation = make_backup_relation(target_relation, backup_relation_type) -%}
  {%- set preexisting_backup_relation = load_cached_relation(backup_relation) -%}
  {% set grant_config = config.get('grants') %}

  {{ drop_relation_if_exists(preexisting_intermediate_relation) }}
  {{ drop_relation_if_exists(preexisting_backup_relation) }}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  -- build model
  {% if language == 'python' %}
    {% call statement('main', language='python', fetch_result=False) -%}
      {{ py_write_table(temporary=False, relation=intermediate_relation, compiled_code=compiled_code) }}
    {%- endcall %}
  {% else %}
    {% call statement('main') -%}
      {{ get_create_table_as_sql(False, intermediate_relation, sql) }}
    {%- endcall %}
  {% endif %}

  {% do create_indexes(intermediate_relation) %}

  -- cleanup
  {% if existing_relation is not none %}
    {% set existing_relation = load_cached_relation(existing_relation) %}
    {% if existing_relation is not none %}
      {{ adapter.rename_relation(existing_relation, backup_relation) }}
    {% endif %}
  {% endif %}

  {{ adapter.rename_relation(intermediate_relation, target_relation) }}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  {% set should_revoke = should_revoke(existing_relation, full_refresh_mode=True) %}
  {% do apply_grants(target_relation, grant_config, should_revoke=should_revoke) %}

  {% do persist_docs(target_relation, model) %}

  {{ adapter.commit() }}

  {{ drop_relation_if_exists(backup_relation) }}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}


{% materialization incremental, adapter='gizmosql', supported_languages=['sql', 'python'] %}

  {% set language = model['language'] %}

  -- delegate to the default incremental materialization for SQL
  {% if language == 'sql' %}
    {% set result = dbt.materialization_incremental_default() %}
    {{ return(result) }}
  {% endif %}

  -- Python incremental: full refresh behavior (rebuild table)
  {%- set existing_relation = load_cached_relation(this) -%}
  {%- set target_relation = this.incorporate(type='table') %}

  {{ drop_relation_if_exists(existing_relation) }}

  {#-- Ensure connection is open for commit --#}
  {%- call statement('setup_connection') -%}
    select 1
  {%- endcall -%}

  {% call statement('main', language='python', fetch_result=False) -%}
    {{ py_write_table(temporary=False, relation=target_relation, compiled_code=compiled_code) }}
  {%- endcall %}

  {% do persist_docs(target_relation, model) %}
  {{ adapter.commit() }}

  {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}
