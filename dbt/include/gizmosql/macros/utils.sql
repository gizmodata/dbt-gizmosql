{% macro gizmosql__dateadd(datepart, interval, from_date_or_timestamp) %}
    {%- set unit = datepart | lower -%}
    {%- if unit == 'quarter' -%}
        ({{ from_date_or_timestamp }} + (cast({{ interval }} as bigint) * 3) * interval 1 month)
    {%- elif unit == 'week' -%}
        ({{ from_date_or_timestamp }} + (cast({{ interval }} as bigint) * 7) * interval 1 day)
    {%- else -%}
        ({{ from_date_or_timestamp }} + cast({{ interval }} as bigint) * interval 1 {{ unit }})
    {%- endif -%}
{% endmacro %}

{% macro gizmosql__last_day(date, datepart) -%}
    {%- if datepart == 'quarter' -%}
    cast(
        {{dbt.dateadd('day', '-1',
        dbt.dateadd('month', '3', dbt.date_trunc(datepart, date))
        )}}
        as date)
    {%- else -%}
    {{dbt.default_last_day(date, datepart)}}
    {%- endif -%}
{%- endmacro %}

{% macro gizmosql__listagg(measure, delimiter_text, order_by_clause, limit_num) -%}
    {% if limit_num -%}
    list_aggr(
        (array_agg(
            {{ measure }}
            {% if order_by_clause -%}
            {{ order_by_clause }}
            {%- endif %}
        ))[1:{{ limit_num }}],
        'string_agg',
        {{ delimiter_text }}
        )
    {%- else %}
    string_agg(
        {{ measure }},
        {{ delimiter_text }}
        {% if order_by_clause -%}
        {{ order_by_clause }}
        {%- endif %}
        )
    {%- endif %}
{%- endmacro %}

{% macro gizmosql__split_part(string_text, delimiter_text, part_number) %}
    string_split({{ string_text }}, {{ delimiter_text }})[{{ part_number }}]
{% endmacro %}

{% macro gizmosql__get_delete_insert_merge_sql(target, source, unique_key, dest_columns, incremental_predicates) -%}

    {%- set dest_cols_csv = get_quoted_csv(dest_columns | map(attribute="name")) -%}

    {% if unique_key %}
        {% if unique_key is sequence and unique_key is not string %}
            delete from {{ target }} as DBT_INCREMENTAL_TARGET
            using {{ source }}
            where (
                {% for key in unique_key %}
                    {{ source }}.{{ key }} = DBT_INCREMENTAL_TARGET.{{ key }}
                    {{ "and " if not loop.last }}
                {% endfor %}
                {% if incremental_predicates %}
                    {% for predicate in incremental_predicates %}
                        and {{ predicate }}
                    {% endfor %}
                {% endif %}
            );
        {% else %}
            delete from {{ target }}
            where (
                {{ unique_key }}) in (
                select ({{ unique_key }})
                from {{ source }}
            )
            {%- if incremental_predicates %}
                {% for predicate in incremental_predicates %}
                    and {{ predicate }}
                {% endfor %}
            {%- endif -%};
        {% endif %}
    {% endif %}

    insert into {{ target }} ({{ dest_cols_csv }})
    (
        select {{ dest_cols_csv }}
        from {{ source }}
    )

{%- endmacro %}

{% macro gizmosql__alter_relation_comment(relation, comment) %}
  {% set escaped_comment = gizmosql_escape_comment(comment) %}
  comment on {{ relation.type }} {{ relation }} is {{ escaped_comment }};
{% endmacro %}

{% macro gizmosql__alter_column_comment(relation, column_dict) %}
  {% set existing_columns = adapter.get_columns_in_relation(relation) | map(attribute="name") | list %}
  {% for column_name in column_dict if (column_name in existing_columns) %}
    {% set comment = column_dict[column_name]['description'] %}
    {% set escaped_comment = gizmosql_escape_comment(comment) %}
    comment on column {{ relation }}.{{ adapter.quote(column_name) if column_dict[column_name]['quote'] else column_name }} is {{ escaped_comment }};
  {% endfor %}
{% endmacro %}

{% macro gizmosql__get_incremental_microbatch_sql(arg_dict) -%}
    {%- set event_time = config.get('event_time') -%}
    {%- if not event_time -%}
        {{ exceptions.raise_compiler_error("microbatch incremental strategy requires an 'event_time' model config") }}
    {%- endif -%}

    {%- set batch_ctx = model.get('batch') -%}
    {%- set batch_start = batch_ctx.get('event_time_start') if batch_ctx else none -%}
    {%- set batch_end = batch_ctx.get('event_time_end') if batch_ctx else none -%}

    {%- if not (batch_start and batch_end) -%}
        {{ exceptions.raise_compiler_error("microbatch requires 'batch.event_time_start' and 'batch.event_time_end' in context") }}
    {%- endif -%}

    {%- set target = arg_dict["target_relation"] -%}
    {%- set source = arg_dict["temp_relation"] -%}
    {%- set dest_columns = arg_dict["dest_columns"] -%}
    {%- set incremental_predicates = arg_dict.get("incremental_predicates") or [] -%}
    {%- set dest_cols_csv = get_quoted_csv(dest_columns | map(attribute="name")) -%}

    {%- set batch_predicate -%}
        {{ event_time }} >= '{{ batch_start }}'
        and {{ event_time }} < '{{ batch_end }}'
    {%- endset -%}

    {%- set where_clause -%}
        {{ batch_predicate }}
        {%- for predicate in incremental_predicates %}
        and ({{ predicate }})
        {%- endfor %}
    {%- endset -%}

    {%- set build_sql -%}
        delete from {{ target }}
        where {{ where_clause }};

        insert into {{ target }} ({{ dest_cols_csv }})
        select {{ dest_cols_csv }}
        from {{ source }}
        where {{ batch_predicate }};
    {%- endset -%}

    {{ return(build_sql) }}
{%- endmacro %}

{% macro gizmosql_escape_comment(comment) -%}
  {% if comment is not string %}
    {% do exceptions.raise_compiler_error('cannot escape a non-string: ' ~ comment) %}
  {% endif %}
  {%- set magic = '$dbt_comment_literal_block$' -%}
  {%- if magic in comment -%}
    {%- do exceptions.raise_compiler_error('The string ' ~ magic ~ ' is not allowed in comments.') -%}
  {%- endif -%}
  {{ magic }}{{ comment }}{{ magic }}
{%- endmacro %}
