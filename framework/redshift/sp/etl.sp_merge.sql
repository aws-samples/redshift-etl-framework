-- Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
-- SPDX-License-Identifier: MIT-0

create or replace procedure etl.sp_merge
  (p_config varchar(8000))
as $$
declare
    v_upsert_type varchar(10);
    v_upsert_keys varchar(500);
    v_staging_refresh_type varchar(10);
    v_partreg_method varchar(10);

    v_delta_schema_name varchar(100);
    v_delta_table_name varchar(100);
    v_staging_schema_name varchar(100);
    v_staging_table_name varchar(100);
    v_target_schema_name varchar(100);
    v_target_table_name varchar(100);

    v_target_type varchar(10);  -- local or external
    v_is_partitioned boolean;
    v_partition_keys varchar(200);

    v_defaults varchar(1000);
    v_default_target_schema_name varchar(100);
    v_default_delta_schema_name varchar(100);
    v_default_staging_schema_name varchar(100);

begin
    raise log 'config=%', p_config;

    -- Get defaults
    call etl.sp_get_config(v_defaults);
    --raise log 'v_defaults=%', v_defaults;
    v_default_target_schema_name   := isnull(json_extract_path_text(v_defaults,'target','schema_name', true),'');
    v_default_delta_schema_name    := isnull(json_extract_path_text(v_defaults,'delta','schema_name', true),'');
    v_default_staging_schema_name  := isnull(json_extract_path_text(v_defaults,'staging','schema_name', true),'etl');

    -- Get schema and table names
    v_target_schema_name    := isnull(json_extract_path_text(p_config,'target','schema_name', true),v_default_target_schema_name);
    v_target_table_name     := isnull(json_extract_path_text(p_config,'target','table_name', true), '');

    if v_target_schema_name = '' then
        raise exception 'ERROR : "target":{"schema_name":"your_schema_name"} is missing in the config';
    end if;

    if v_target_table_name = '' then
        raise exception 'ERROR : "target":{"table_name":"your_table_name"} is missing in the config';
    end if;

    v_delta_schema_name     := isnull(json_extract_path_text(p_config,'delta','schema_name', true),v_default_delta_schema_name);
    v_delta_table_name      := isnull(json_extract_path_text(p_config,'delta','table_name', true), v_target_table_name + '_delta');
    v_staging_schema_name   := isnull(json_extract_path_text(p_config,'staging','schema_name', true),v_default_staging_schema_name);
    v_staging_table_name    := isnull(json_extract_path_text(p_config,'staging','table_name', true), v_target_table_name + '_stg');

    -- Get options
    v_upsert_type           := isnull(json_extract_path_text(p_config,'upsert','type', true),'pk');
    v_upsert_keys           := isnull(json_extract_path_text(p_config,'upsert','keys', true),'');
    v_staging_refresh_type  := isnull(json_extract_path_text(p_config,'staging','refresh_type', true),'');
    v_partreg_method        := isnull(json_extract_path_text(p_config,'target','partition_registration', true),'msck');

    raise log $log$v_target_schema_name=%
                   v_target_table_name=%,
                   v_delta_schema_name=%,
                   v_delta_table_name=%,
                   v_staging_schema_name=%,
                   v_staging_table_name=%,
                   v_upsert_type=%,
                   v_upsert_keys=%,
                   v_staging_refresh_type=%,
                   v_partreg_method=%$log$,
                   v_target_schema_name,
                   v_target_table_name,
                   v_delta_schema_name,
                   v_delta_table_name,
                   v_staging_schema_name,
                   v_staging_table_name,
                   v_upsert_type,
                   v_upsert_keys,
                   v_staging_refresh_type,
                   v_partreg_method;

    -- Get target type
    select into v_target_type schema_type
    from SVV_ALL_SCHEMAS
    where database_name = current_database()
      and schema_name = v_target_schema_name;


    if isnull(v_target_type,'') = '' then
        raise exception 'ERROR : Target schema % cannot be found!', v_target_schema_name;
    end if;

    if v_target_type = 'local' then
        -- If target is local, staging table will be also the target table
        v_staging_schema_name := v_target_schema_name;
        v_staging_table_name := v_target_table_name;
    end if;

    if v_target_type = 'external' then
        -- Get the partitioned columns
        drop table if exists tmp_partition_columns;
        create temp table tmp_partition_columns
        as
        select columnname, part_key
        from SVV_EXTERNAL_COLUMNS
        where schemaname = v_target_schema_name
          and tablename = v_target_table_name
          and part_key > 0;

        select into v_partition_keys 'a.' || listagg(columnname,', a.') within group (order by part_key)
        from tmp_partition_columns;

        v_is_partitioned:=exists(select * from tmp_partition_columns);

        -- If table is partitioned;
        if v_is_partitioned = true then
            if not v_partreg_method in ('msck','spectrum') then
                raise exception 'ERROR : New partition registration method can be "msck" or "spectrum"';
            end if;

            call etl.sp_merge_refresh_staging
                (v_delta_schema_name,
                v_delta_table_name,
                v_staging_schema_name,
                v_staging_table_name,
                v_target_schema_name,
                v_target_table_name,
                v_partition_keys,
                v_staging_refresh_type);

        end if;
    end if; -- # if v_target_type = 'external'

    if v_upsert_type <> 'none' then
        call etl.sp_merge_upsert
            (v_delta_schema_name,
             v_delta_table_name,
             v_staging_schema_name,
             v_staging_table_name,
             v_upsert_type,
             v_upsert_keys);


    end if;

    if v_target_type = 'external' then
        call etl.sp_merge_unload
            (v_staging_schema_name,
             v_staging_table_name,
             v_target_schema_name,
             v_target_table_name,
             v_is_partitioned,
             v_partition_keys,
             v_partreg_method,
             v_iam_role_arn);

    end if;

    drop table if exists tmp_partition_columns;
end;

$$ language plpgsql;