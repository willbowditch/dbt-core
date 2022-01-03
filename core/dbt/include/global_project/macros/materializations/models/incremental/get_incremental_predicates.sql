{#

    These macros will compile the proper join predicates for incremental models,
    merging default behavior with the optional, user-supplied incremental_predicates
    config. 

#}

{% macro get_incremental_predicates(target_relation, incremental_strategy, unique_key, user_predicates, partitions=none) %}
    {{ adapter.dispatch('get_incremental_predicates')(target_relation, incremental_strategy, unique_key, user_predicates, partitions) }}
{% endmacro %}

{% macro default__get_incremental_predicates(target_relation, incremental_strategy, unique_key, user_predicates=none, partitions=none) %}
    {#
    
        This behavior should only be observed when dbt calls the default
        `get_delete_insert_merge_sql` strategy in dbt-core
    
    #}
    {%- if user_predicates -%}
        {%- set predicates %}
            {%- for condition in user_predicates -%} and {{ target_relation.name }}.{{ condition.source_col }} {{ condition.expression }} {% endfor -%} 
        {%- endset -%}
    {%- endif -%}

    {{ return(predicates) }}

{% endmacro %}