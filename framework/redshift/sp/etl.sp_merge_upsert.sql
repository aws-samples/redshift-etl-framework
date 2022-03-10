-- Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
-- SPDX-License-Identifier: MIT-0

create or replace procedure etl.sp_merge_upsert
  (p_delta_schema_name varchar(100),
   p_delta_table_name varchar(100),
   p_staging_schema_name varchar(100),
   p_staging_table_name varchar(100),
   p_option_upsert_type varchar(10),
   p_option_upsert_keys varchar(500)
  )
as $$
declare 
    v_sql varchar(10000);
    v_pkey varchar(100);
    v_key_array varchar(200);
    v_key_array_length int;
    v_counter int;
	v_column_name varchar(100);
    
begin        
    if p_option_upsert_type = 'pk' then
        -- Find the PK
        select into v_pkey kcu.column_name
        from information_schema.table_constraints tco
        join information_schema.key_column_usage kcu
            on kcu.constraint_name = tco.constraint_name
            and kcu.constraint_schema = tco.constraint_schema
            and kcu.constraint_name = tco.constraint_name
        where tco.table_schema = p_staging_schema_name
            and tco.table_name = p_staging_table_name
            and tco.constraint_type = 'PRIMARY KEY';

        if isnull(v_pkey,'') = '' then
            raise exception 'ERROR : Table % does not have a Primary Key defined!', p_staging_table_name;
        end if;
    end if;

    -- Perform UPSERT
    if p_option_upsert_type = 'all' then
        v_sql:='truncate ' || p_staging_schema_name || '.' || p_staging_table_name;
    else
        if p_option_upsert_type = 'pk' then
            v_key_array = '["' || v_pkey || '"]';
        elsif p_option_upsert_keys <> '' then
            v_key_array = p_option_upsert_keys;
        end if;

        raise log 'p_option_upsert_keys=%' , p_option_upsert_keys;

        v_key_array_length = json_array_length(v_key_array, true);
        if v_key_array_length = 0 then
            raise exception 'ERROR : Merge keys can not be found!'; 
        end if;

        v_sql:='delete from ' || p_staging_schema_name || '.' || p_staging_table_name
            || ' using ' || p_delta_schema_name || '.' || p_delta_table_name || ' d'
            || ' where ';

        raise log 'v_key_array_length=%',v_key_array_length;
        
        v_counter = 0;
        while v_counter<v_key_array_length
        loop
            v_column_name=json_extract_array_element_text(v_key_array, v_counter, true);

            if isnull(v_column_name,'') = '' then
                raise exception 'ERROR : Merge column name cannot be found!'; 
            end if;
            
            if v_counter>0 then
                v_sql:=v_sql + ' and';
            end if;

            v_sql:=v_sql + ' d.' || v_column_name || ' = ' || p_staging_schema_name || '.' || p_staging_table_name || '.' || v_column_name;

            v_counter = v_counter + 1;
        end loop;

    end if;

    --raise info 'SQL: Delete: %', v_sql;
    execute(v_sql);

    -- Reinsert new & updated rows
    v_sql:='insert into ' || p_staging_schema_name || '.' || p_staging_table_name
        || ' select * from ' || p_delta_schema_name || '.' || p_delta_table_name;

    execute(v_sql);
end;
$$ language plpgsql;