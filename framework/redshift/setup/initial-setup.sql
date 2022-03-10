-- Create ETL schema
create schema if not exists etl;

-- ETL Config Defaults
create table etl.config (
   item varchar(100),
   value varchar(500)
)
    diststyle all
    sortkey (item);

insert into etl.config (item, value) values
('source-schema-name', '<your_external_schema_for_source_tables>'),
('ingest-schema-name', '<your_internal_schema_for_data_ingestion>'),
('delta-schema-name', '<your_internal_schema_for_delta_tables>'),
('staging-schema-name', '<your_internal_schema_for_staging_tables>'),
('target-schema-name', '<your_external_schema_for_target_tables'),
('unload-iam-role', 'iam_role":"arn:aws:iam::<your_account_id>:role/<your_iam_role>');
