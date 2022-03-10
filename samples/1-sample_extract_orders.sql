call etl.sp_ingest('
{
    "source":{
        "schema_name":"datalake",
        "table_name":"orders_src"
    },
    "ingest":{
        "schema_name":"etl",
        "table_name":"orders_src"
    }
}
');