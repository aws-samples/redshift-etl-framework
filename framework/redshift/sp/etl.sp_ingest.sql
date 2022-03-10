-- Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
-- SPDX-License-Identifier: MIT-0

create or replace procedure etl.sp_ingest
    (p_config varchar(8000))
as $$
declare
    v_source_schema_name varchar(100);
    v_source_table_name varchar(100);
    v_source_column_names varchar(1000);
    v_source_custom_where_sql varchar(1000);
    v_ingest_schema_name varchar(100);
    v_ingest_table_name varchar(100);

    v_default_source_schema_name varchar(100);
    v_default_ingest_schema_name varchar(100);

    v_sql varchar(1000);
    v_defaults varchar(1000);
begin
    raise log 'config=%', p_config;

    -- Get defaults
    call etl.sp_get_config(v_defaults);
    --raise log 'v_defaults=%', v_defaults;
    v_default_source_schema_name    := isnull(json_extract_path_text(v_defaults,'source','schema_name', true),'');
    v_default_ingest_schema_name    := isnull(json_extract_path_text(v_defaults,'ingest','schema_name', true),'');

    -- Get schema and table names
    v_source_schema_name        := isnull(json_extract_path_text(p_config,'source','schema_name', true),v_default_source_schema_name);
    v_source_table_name         := isnull(json_extract_path_text(p_config,'source','table_name', true), '');
    v_source_column_names       := isnull(json_extract_path_text(p_config,'source','column_names', true), '');
    v_source_custom_where_sql   := isnull(json_extract_path_text(p_config,'source','custom_where_sql', true), '');

    v_ingest_schema_name    := isnull(json_extract_path_text(p_config,'ingest','schema_name', true),v_default_ingest_schema_name);
    v_ingest_table_name     := isnull(json_extract_path_text(p_config,'ingest','table_name', true), '');


    if v_source_column_names = '' then
        v_source_column_names := '*';
    end if;

    raise log $log$v_source_schema_name=%,
                   v_source_table_name=%,
                   v_source_column_names=%,
                   v_source_custom_where_sql=%,
                   v_ingest_schema_name=%
                   v_ingest_table_name=%$log$,
            v_source_schema_name,
            v_source_table_name,
            v_source_column_names,
            v_source_custom_where_sql,
            v_ingest_schema_name,
            v_ingest_table_name;

    if v_source_schema_name = ''
    or v_source_table_name = ''
    or v_ingest_schema_name = ''
    or v_ingest_table_name = ''then
        raise exception 'ERROR : Source or Ingest schema or table name is missing. Please check the config parameter!';
    end if;

    v_sql:='truncate table ' || v_ingest_schema_name || '.' || v_ingest_table_name;

    execute (v_sql);

    v_sql:='insert into ' || v_ingest_schema_name || '.' || v_ingest_table_name
        || ' select ' || v_source_column_names || ' from ' || v_source_schema_name || '.' || v_source_table_name;

    if v_source_custom_where_sql <> '' then
        v_sql := v_sql
            || ' where ' || v_source_custom_where_sql;
    end if;

    --raise log 'SQL: %', v_sql;
    execute (v_sql);
end;

$$ language plpgsql;