-- Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
-- SPDX-License-Identifier: MIT-0

create or replace procedure etl.sp_datalake_table_refresh
  (p_ext_schemaname varchar(100),
   p_tablename varchar(100),
   p_options varchar(100) -- optional, json format
        -- staging
            -- reload : Reloads the partion or table from datalake
        -- upsert
            -- pk: Use Primary keys (default)
            -- all: Use all records from delta
            -- none: Don't perform upsert
            -- key: ["column1, column2"]: Use specified columns to upsert
        -- partreg_method
            -- msck: New partitions will be registered by running MSCK TABLE REPAIR in Athena (default)
            -- spectrum: New partitions will be registered by running INSERT INTO TargetTable in Redshift 
            
  )
as $$
declare 
    option_upsert varchar;
    option_upsert_keys varchar;
    option_staging varchar;
    option_partreg_method varchar;

	sql varchar(10000);
    rec record;
    v_is_partitioned boolean;
    v_partition_keys varchar(200);    
    v_table_location varchar(500);
    v_result varchar(100);
begin
    -- Get options
    option_upsert:=isnull(json_extract_path_text(p_options,'upsert', true),'pk');
    option_upsert_keys:=isnull(json_extract_path_text(option_upsert,'key', true),'');
    option_staging:=isnull(json_extract_path_text(p_options,'staging', true),'');
    option_partreg_method:=isnull(json_extract_path_text(p_options,'partreg_method', true),'msck');

    -- Get the partitioned columns 
    drop table if exists tmp_partition_columns;
    create temp table tmp_partition_columns
    as
    select columnname, part_key
    from SVV_EXTERNAL_COLUMNS
    where schemaname = p_ext_schemaname
      and tablename = p_tablename
      and part_key > 0;

    select into v_partition_keys 'a.' || listagg(columnname,', a.') within group (order by part_key)
    from tmp_partition_columns;
    
    v_is_partitioned:=exists(select * from tmp_partition_columns);
  
    -- If table is partitioned;
    if v_is_partitioned = true then
        if not option_partreg_method in ('msck','spectrum') then
            raise exception 'ERROR : New partition registerition method can be "msck" or "spectrum"'; 
        end if;

        call etl.sp_datalake_table_refresh_staging(
            p_ext_schemaname,
            p_tablename,
            v_partition_keys,
            option_staging);

    end if; -- # if v_is_partitioned = true

    if option_upsert <> 'none' then
        call etl.sp_datalake_table_refresh_upsert (
            p_ext_schemaname,
            p_tablename,
            option_upsert,
            option_upsert_keys);

    end if; 
    
    if v_is_partitioned = true and option_partreg_method = 'spectrum' then

        call etl.sp_datalake_table_refresh_add_partitions (
            p_ext_schemaname,
            p_tablename,
            v_partition_keys);
      
    end if; --if v_is_partitioned = true 
  
    -- Unload
    select into v_table_location e.location 
    from svv_external_tables e
    where e.schemaname = p_ext_schemaname
      and e.tablename = p_tablename;
  
  
    sql:='unload (''select * from etl.' || p_tablename || '_stg'')'
      || ' to ''' || v_table_location || ''''
      || ' iam_role ''arn:aws:iam::452964029168:role/udrRedshiftS3FullAccess'''
      || ' parquet';
    
    if v_is_partitioned = true then
        sql:=sql || ' partition BY (' || replace(v_partition_keys,'a.','') || ')';
    end if;
    
    sql:=sql || ' cleanpath';
    --raise info 'SQL1 : %', sql;
    execute(sql);
  
    -- Register new table partitions
    if v_is_partitioned = true and option_partreg_method = 'msck' then
        select into v_result etl.exfunc_add_partitions(p_tablename);
    end if;

    drop table if exists tmp_partitions_delta;
    drop table if exists tmp_partitions_stg_delete;
    drop table if exists tmp_partitions_add;  
    drop table if exists tmp_partition_columns;
    drop table if exists tmp_partitions_stg;
  
end;

$$ language plpgsql;