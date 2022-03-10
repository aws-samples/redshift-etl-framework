-- Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
-- SPDX-License-Identifier: MIT-0

create or replace procedure etl.sp_get_config
    (p_config inout varchar(8000))
as $$
declare
    v_default_source_schema_name varchar(100);
    v_default_ingest_schema_name varchar(100);
    v_default_delta_schema_name varchar(100);
    v_default_staging_schema_name varchar(100);
    v_default_target_schema_name varchar(100);
    v_unload_iam_role varchar(500);
begin
    select into v_default_source_schema_name value
    from etl.config
    where item = 'source-schema-name';

    select into v_default_ingest_schema_name value
    from etl.config
    where item = 'ingestion-schema-name';

    select into v_default_delta_schema_name value
    from etl.config
    where item = 'delta-schema-name';

    select into v_default_staging_schema_name value
    from etl.config
    where item = 'staging-schema-name';

    select into v_default_target_schema_name value
    from etl.config
    where item = 'target-schema-name';

    select into v_unload_iam_role value
    from etl.config
    where item = 'unload-iam-role';

    p_config:='{'
                  || '"default_source_schema_name":"'  || v_default_source_schema_name || '",'
                  || '"default_ingest_schema_name":"'  || v_default_ingest_schema_name || '",'
                  || '"default_delta_schema_name":"'   || v_default_delta_schema_name || '",'
                  || '"default_staging_schema_name":"' || v_default_staging_schema_name || '",'
                  || '"default_target_schema_name":"'  || v_default_target_schema_name || '",'
                  || '"unload_iam_role":"'             || v_unload_iam_role || '"'
        || '}';

end;
$$ language plpgsql;