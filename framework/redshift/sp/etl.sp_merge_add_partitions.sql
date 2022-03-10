-- Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
-- SPDX-License-Identifier: MIT-0

create or replace procedure etl.sp_merge_add_partitions
  (p_target_schema_name varchar(100),
   p_target_table_name varchar(100),
   p_partition_keys varchar(500)
  )
as $$
declare 
    v_sql varchar(10000);
    v_rec record;
    v_counter int;
    v_result varchar(100);
begin
    -- Check if new partitions need to be added 
    drop table if exists tmp_partitions_to_be_added;  
    v_sql:='create temp table tmp_partitions_to_be_added'
        || ' as'
        || ' select ' || p_partition_keys
        || ' from tmp_partitions_stg a'
        || ' left join svv_external_partitions e'
        || '   on e.schemaname = ''' || p_target_schema_name || ''''
        || '  and e.tablename = ''' || p_target_table_name || ''''
        || '  and e.values = ''[';

    -- tmp_partitions_stg is created in etl.sp_merge_refresh_staging
    v_counter:=0;
    
    for v_rec in
        select columnname
        from tmp_partition_columns
        order by part_key
        -- tmp_partition_columns is created in etl.sp_merge SP
    loop
        v_counter:=v_counter+1;
        if v_counter > 1 then
            v_sql:=v_sql+ ',';
        end if;         
        v_sql:=v_sql+ '"'' || a.' || rec.columnname || ' || ''"';

    end loop;

    v_sql:=v_sql+ ']''';
    v_sql:=v_sql+ ' where e.values is null';
    
    --raise info 'SQL:tmp_partitions_to_be_added : %', v_sql;
    execute(v_sql);
    
    v_sql:='insert into ' || p_target_schema_name || '.' || p_target_table_name || ' select ';
    v_counter = 0;
    for v_rec in
        select columnname, external_type
        from svv_external_columns
        where schemaname = p_target_schema_name
          and tablename = p_target_table_name
        order by columnnum
    loop
        v_counter:=v_counter+1;

        if v_counter = 1 then
            v_sql:=v_sql+ ' ';
        else
            v_sql:=v_sql+ ',';
        end if;
        
        if exists (select * from tmp_partition_columns where columnname = rec.columnname) then
            v_sql:=v_sql+ 'cast(' || rec.columnname || ' as ' || rec.external_type || ') as ' || rec.columnname;
        else         
            v_sql:=v_sql+ 'cast(null as ' || rec.external_type || ') as ' || rec.columnname;
        end if;

    end loop;
    v_sql:=v_sql+ ' from tmp_partitions_to_be_added';
    
    --raise info 'SQL:add partitions : %', v_sql;

    select into v_result exfunc_redshift_executer(sql);

    drop table if exists tmp_partitions_to_be_added;
end;

$$ language plpgsql;