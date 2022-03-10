call etl.sp_ingest('
{
    "target": {
        "schema_name": "datalake",
        "table_name": "orders",
        "partition_registration": "msck"
    },
    "delta": {
        "schema_name": "etl",
        "table_name": "orders_delta"
    },
    "staging": {
        "schema_name": "etl",
        "table_name": "orders_stg"
    },
    "upsert": {
        "type": "pk"
    }
}
');