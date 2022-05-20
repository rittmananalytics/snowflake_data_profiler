# snowflake_data_profiler
Hex data app to browse and profile the contents of schema and table(s) in a Snowflake database.

![](img/data_profiler_screenshot.png)

As well as showing basic schema information and metadata from the INFORMATION_SCHEMA, this notebook runs our Snowflake data profiling SQL as detailed below (note the {{table_name | sqlsafe }} and other Jinja variables used to insert the selected database, schema and table name values from the app into the SQL query):

```
SELECT column_name,
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
  FROM
 (
  WITH
    `table` AS (SELECT * FROM {{ selected_database | sqlsafe }}.{{table_schema | sqlsafe }}.{{table_name | sqlsafe }} sample ({{sample_size}})),
    table_as_json AS (SELECT OBJECT_CONSTRUCT(t.*)  FROM `table` t ),
    pairs AS (select t.key as column_name, replace(replace(t.value,'"',''),'') column_value from table_as_json s, table(flatten(s.$1)) t ),
    profile AS (
      SELECT
        split_part(replace('{{ selected_database | sqlsafe }}.{{table_schema | sqlsafe }}.{{table_name | sqlsafe }}','"',''),'.',1 ) as table_catalog,
        split_part(replace('{{ selected_database | sqlsafe }}.{{table_schema | sqlsafe }}.{{table_name | sqlsafe }}','"',''),'.',2 ) as table_schema,
        split_part(replace('{{ selected_database | sqlsafe }}.{{table_schema | sqlsafe }}.{{table_name | sqlsafe }}','"',''),'.',3 ) as table_name,
        column_name,
        COUNT(*) AS table_rows,
        COUNT(DISTINCT column_value) AS count_distinct_values,
        (div0(COUNT(DISTINCT column_value),COUNT(*)))::float AS pct_unique,
        case when column_value IS NULL then 1 else 0 end AS _nulls,
        case when column_value IS NOT NULL then 1 else 0 end  AS _non_nulls,
        (case when column_value IS NOT NULL then 1 else 0 end  / COUNT(*))::float AS pct_not_null,
        min(column_value) as _min_value,
        max(column_value) as _max_value,
        avg(try_cast(column_value as number)) as _avg_value,
        mode(column_value) AS _most_frequent_value,
        MIN(LENGTH(column_value::varchar)) AS _min_length,
        MAX(LENGTH(column_value::varchar)) AS _max_length,
        ROUND(AVG(LENGTH(column_value::varchar))) AS _avr_length
      FROM
        pairs
      WHERE
        column_name <> ''
        AND column_name NOT LIKE '%-%'
      GROUP BY
        1,2,3,4,8,9
      ORDER BY
        5 desc,1,2,3,4,8,9
  )
  SELECT
    *
  FROM
    profile)
  order by column_name
```
