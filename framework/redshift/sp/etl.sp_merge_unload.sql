-- Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
-- SPDX-License-Identifier: MIT-0

create or replace procedure etl.sp_merge_unload
  (p_staging_schema_name varchar(100),
   p_staging_table_name varchar(100),
   p_target_schema_name varchar(100),
   p_target_table_name varchar(100),
   p_is_partitioned boolean,
   p_partition_keys varchar(200),
   p_partreg_method varchar(10))
as $$
declare
    v_sql varchar(10000);
    v_iam_role_arn varchar(200);
    v_table_location varchar(500);
    v_result varchar(100);
    v_defaults varchar(1000);
begin
    -- Get defaults
    call etl.sp_get_config(v_defaults);

    v_iam_role_arn := isnull(json_extract_path_text(v_defaults,'unload-iam-role', true),'');

    if v_iam_role_arn = '' then
        raise exception 'ERROR : unload-iam-role is not defined in etl.config table';
    end if;

    if p_is_partitioned = true
    and p_partreg_method = 'spectrum' then

        call etl.sp_merge_add_partitions
            (p_target_schema_name,
             p_target_table_name,
             p_partition_keys);
    end if;

    -- Unload
    select into v_table_location e.location
    from svv_external_tables e
    where e.schemaname = p_target_schema_name
      and e.tablename = p_target_table_name;

    v_sql:='unload (''select * from ' || p_staging_schema_name || '.' || p_staging_table_name || ''')'
        || ' to ''' || v_table_location || ''''
        || ' iam_role ''' || v_iam_role_arn || ''''
        || ' parquet';

    if p_is_partitioned = true then
        v_sql:=v_sql || ' partition BY (' || replace(p_partition_keys,'a.','') || ')';
    end if;

    v_sql:=v_sql || ' cleanpath';
    raise info 'Unload: %', v_sql;
    execute(v_sql);

    -- Register new table partitions
    if p_is_partitioned = true and p_partreg_method = 'msck' then
        select into v_result etl.exfunc_add_partitions(p_target_table_name);
    end if;
end;

$$ language plpgsql;