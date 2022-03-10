-- Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
-- SPDX-License-Identifier: MIT-0

create or replace procedure etl.sp_merge_refresh_staging(
    p_delta_schema_name varchar(100),
    p_delta_table_name varchar(100),
    p_staging_schema_name varchar(100),
    p_staging_table_name varchar(100),
    p_target_schema_name varchar(100),
    p_target_table_name varchar(100),
    p_partition_keys varchar(500),
    v_staging_refresh_type varchar(10))
as $$
declare
    v_sql varchar(10000);
    v_rec record;
    v_join_condition varchar;
    v_counter int;
	v_column_name varchar(100);
begin
    drop table if exists tmp_partitions_delta;
    v_sql:='create temp table tmp_partitions_delta'
        || ' as'
        || ' select distinct ' || p_partition_keys
        || ' from ' || p_delta_schema_name ||'.' || p_delta_table_name || ' a';

    raise info 'tmp_partitions_delta : %', v_sql;
    execute(v_sql);

    drop table if exists tmp_partitions_stg;
    v_sql:='create temp table tmp_partitions_stg'
        || ' as'
        || ' select distinct ' || p_partition_keys
        || ' from ' || p_staging_schema_name || '.' || p_staging_table_name || ' a';

    raise info 'tmp_partitions_stg : %', v_sql;
    execute(v_sql);

    -- Join Conditions
    v_join_condition:='';
    v_counter:=0;

    for v_rec in
    select columnname from tmp_partition_columns order by part_key
    -- tmp_partition_columns is created in etl.sp_merge SP
    loop
        v_counter:=v_counter+1;

        if v_counter = 1
        then
            v_join_condition:=v_join_condition+' on';
        else
            v_join_condition:=v_join_condition+' and';
        end if;

        v_column_name:=v_rec.columnname;
        v_join_condition:=v_join_condition+' a.' || v_column_name || ' = b.' || v_column_name;

    end loop;

    -- Check if there are inactive partitions in the stage table
    drop table if exists tmp_partitions_stg_delete;
    v_sql:='create temp table tmp_partitions_stg_delete'
        || ' as'
        || ' select ' || p_partition_keys
        || ' from tmp_partitions_stg a'
        || ' left join tmp_partitions_delta b'
        || v_join_condition
        || ' where b.' || v_column_name || ' is null';

    --raise info 'tmp_partitions_stg_delete : %', v_sql;
    execute(v_sql);
    --raise info 'option_staging : %', p_option_staging;

    if v_staging_refresh_type = 'reload' then
        insert into tmp_partitions_stg_delete
        select * from tmp_partitions_delta;

        v_sql:='delete from tmp_partitions_stg'
            || ' using tmp_partitions_stg b'
            || ' join tmp_partitions_stg_delete a'
            || v_join_condition;

        --raise info 'SQL-Reload : %', v_sql;
        execute(v_sql);

    end if;

    -- Delete inactive partitions from stage table
    if exists (select * from tmp_partitions_stg_delete) then
        v_sql:='delete from ' || p_staging_schema_name || '.' || p_staging_table_name
            || ' using ' || p_staging_schema_name || '.' || p_staging_table_name || ' b'
            || ' join tmp_partitions_stg_delete a'
            || v_join_condition;

        execute(v_sql);
    end if;

    -- Check if new partitions should be loaded from datalate to the staging table
    drop table if exists tmp_partitions_load;
    v_sql:='create temp table tmp_partitions_load'
        || ' as'
        || ' select ' || p_partition_keys
        || ' from tmp_partitions_delta a'
        || ' left join tmp_partitions_stg b'
        || v_join_condition
        || ' where b.' || v_column_name || ' is null';

    execute(v_sql);

    if exists(select * from tmp_partitions_load) then
        v_sql:='insert into ' || p_staging_schema_name || '.' || p_staging_table_name
            || ' select b.*'
            || ' from ' || p_target_schema_name || '.' || p_target_table_name || ' b'
            || ' join tmp_partitions_load a'
            || v_join_condition;

        execute(v_sql);
    end if;
end;

$$ language plpgsql;

