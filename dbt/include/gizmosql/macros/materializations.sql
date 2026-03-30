{#
  GizmoSQL materialization overrides.

  The default dbt table and view materializations use a create-tmp-then-rename
  pattern (CREATE __dbt_tmp → RENAME existing → __dbt_backup → RENAME __dbt_tmp
  → target). Over Flight SQL, the PREPARE phase of subsequent statements can
  see a stale catalog that doesn't reflect uncommitted DDL from earlier in the
  same transaction, causing sporadic "Catalog Error: Table ... does not exist"
  failures on remote GizmoSQL instances.

  DuckDB supports CREATE OR REPLACE for both tables and views, so we override
  the materializations to use that directly on the target relation, eliminating
  the intermediate rename dance entirely.
#}

{% materialization table, adapter='gizmosql' %}

  {%- set existing_relation = load_cached_relation(this) -%}
  {%- set target_relation = this.incorporate(type='table') -%}
  {%- set grant_config = config.get('grants') -%}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  -- `BEGIN` happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  -- Drop existing relation if it is a view (type mismatch)
  {% if existing_relation is not none and existing_relation.type != 'table' %}
    {{ adapter.drop_relation(existing_relation) }}
    {% set existing_relation = none %}
  {% endif %}

  -- build model using CREATE OR REPLACE directly on target
  {% call statement('main') -%}
    {{ gizmosql__create_or_replace_table(target_relation, sql) }}
  {%- endcall %}

  {% do create_indexes(target_relation) %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  {% set should_revoke = should_revoke(existing_relation, full_refresh_mode=True) %}
  {% do apply_grants(target_relation, grant_config, should_revoke=should_revoke) %}

  {% do persist_docs(target_relation, model) %}

  {{ adapter.commit() }}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}


{% materialization view, adapter='gizmosql' %}

  {%- set existing_relation = load_cached_relation(this) -%}
  {%- set target_relation = this.incorporate(type='view') -%}
  {%- set grant_config = config.get('grants') -%}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  -- Drop existing relation if it is a table (type mismatch)
  {% if existing_relation is not none and existing_relation.type != 'view' %}
    {{ adapter.drop_relation(existing_relation) }}
    {% set existing_relation = none %}
  {% endif %}

  -- `BEGIN` happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  -- build model using CREATE OR REPLACE directly on target
  {% call statement('main') -%}
    {{ gizmosql__create_or_replace_view(target_relation, sql) }}
  {%- endcall %}

  {% set should_revoke = should_revoke(existing_relation, full_refresh_mode=True) %}
  {% do apply_grants(target_relation, grant_config, should_revoke=should_revoke) %}

  {% do persist_docs(target_relation, model) %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  {{ adapter.commit() }}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}


{% macro gizmosql__create_or_replace_table(relation, compiled_code) %}
  {% set contract_config = config.get('contract') %}
  {% if contract_config.enforced %}
    {{ get_assert_columns_equivalent(compiled_code) }}
  {% endif %}
  {%- set sql_header = config.get('sql_header', none) -%}

  {{ sql_header if sql_header is not none }}

  create or replace table {{ relation }}
  {% if contract_config.enforced %}
    {{ get_table_columns_and_constraints() }} ;
    insert into {{ relation }} {{ get_column_names() }} (
      {{ get_select_subquery(compiled_code) }}
    );
  {% else %}
    as (
      {{ compiled_code }}
    );
  {% endif %}
{% endmacro %}


{% macro gizmosql__create_or_replace_view(relation, sql) %}
  {% set contract_config = config.get('contract') %}
  {% if contract_config.enforced %}
    {{ get_assert_columns_equivalent(sql) }}
  {% endif %}
  {%- set sql_header = config.get('sql_header', none) -%}

  {{ sql_header if sql_header is not none }}
  create or replace view {{ relation }} as (
    {{ sql }}
  );
{% endmacro %}
