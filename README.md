# Hex Data App Snowflake Data Profiler

Repo contains a [Hex data app](https://hex.tech/) to browse and profile the contents of schema and table(s) in a Snowflake database.

![](img/data_profiler_screenshot.png)

As well as showing basic schema information and metadata from the INFORMATION_SCHEMA, this notebook runs our Snowflake data profiling SQL as detailed below (note the ``{{table_name | sqlsafe }}`` and other Jinja variables used to insert the selected database, schema and table name values from the app into the SQL query):

```
select column_name,
       table_rows::integer as values_sampled,
       count_distinct_values::integer as unique_values_sample,
       pct_unique::float as pct_unique,
       pct_not_null::float as pct_not_null,
       _min_value as min_value,
       _max_value as max_value,
       _avg_value as avg_value,
       _most_frequent_value as mf_value,
       _min_length as min_length,
       _max_length as max_length,
       _avr_length as avg_length
  from
 (
  with
    `table` as (select * from {{ selected_database | sqlsafe }}.{{table_schema | sqlsafe }}.{{table_name | sqlsafe }} sample ({{sample_size}})),
    table_as_json as (select object_construct(t.*)  from `table` t ),
    pairs as (select t.key as column_name, replace(replace(t.value,'"',''),'') column_value from table_as_json s, table(flatten(s.$1)) t ),
    profile as (
      select
        split_part(replace('{{ selected_database | sqlsafe }}.{{table_schema | sqlsafe }}.{{table_name | sqlsafe }}','"',''),'.',1 ) as table_catalog,
        split_part(replace('{{ selected_database | sqlsafe }}.{{table_schema | sqlsafe }}.{{table_name | sqlsafe }}','"',''),'.',2 ) as table_schema,
        split_part(replace('{{ selected_database | sqlsafe }}.{{table_schema | sqlsafe }}.{{table_name | sqlsafe }}','"',''),'.',3 ) as table_name,
        column_name,
        count(*) as table_rows,
        count(distinct column_value) as count_distinct_values,
        (div0(count(distinct column_value),count(*)))::float as pct_unique,
        case when column_value is null then 1 else 0 end as _nulls,
        case when column_value is not null then 1 else 0 end  as _non_nulls,
        (case when column_value is not null then 1 else 0 end  / count(*))::float as pct_not_null,
        min(column_value) as _min_value,
        max(column_value) as _max_value,
        avg(try_cast(column_value as number)) as _avg_value,
        mode(column_value) as _most_frequent_value,
        min(length(column_value::varchar)) as _min_length,
        max(length(column_value::varchar)) as _max_length,
        round(avg(length(column_value::varchar))) as _avr_length
      from
        pairs
      where
        column_name <> ''
        and column_name not like '%-%'
      group by
        1,2,3,4,8,9
      order by
        5 desc,1,2,3,4,8,9
  )
  select
    *
  from
    profile)
  order by column_name
```
