
# Redshift ETL Framework for Data Lakes and Warehouses

A lightweight and config driven ETL (extract, transform, load) framework to perform ETL, ELT and ELTL operations on data lakes using Amazon Redshift


## Features

- Full table refresh on data lake tables
- Partition level refresh on data lake tables
- Supports ETL, ELT, and ELTL techniques
- Config driven ETL framework 


## Built with
- Amazon Redshift
- Amazon Lambda
- Amazon Athena

## Setup

- Download the code repository
- Connect to your Redshift cluster
- Run framework/redshift/setup/initial_setup.sql (provide valid values for etl.config records)
- Create a Lambda function, athena-msck-table-repair, by using the framework/lambda/athena-msck-table-repair.py
- Run framework/redshift/udf/*.sql scripts to create framework UDFs (replace <your_account_id> with your account id)
- Run framework/redshift/sp/*.sql scripts to create framework stored procedures
 

## Usage

You can use the framework for :

1 - Extract and ingest data from data lake source tables

2 - Merge (update and inserts) and Load data to data lake target tables


### Extact and Ingest :
```sql
call etl.sp_ingest(<config>); 
```

### Merge and Load :
```sql
call etl.sp_merge(<config>); 
```

## Config Parameter

Config parameter is a json string which contains a set of parameters and their values to perform the ETL operations

### Extract and Ingest (copy from data lake to Redshift)
```
{
    "source": {
        "schema_name": "....",
        "table_name": "....",
        "column_names": "....",
        "custom_where_sql": "...."
    },
    "ingest": {
        "schema_name": "....",
        "table_name": "...."
    }
}
```

| Key    |Sub Key| Description                                                          | Type         | Default |
|--------|---|----------------------------------------------------------------------|--------------|---------|
||
| source | | A data lake table where you want to extract data                     | Required     |         |
|        |schema_name| Redshift External schema where the source table exists               | Optional (*) |         |
|        |table_name| Redshift External table where pointing to the source data            | Required     |         |
|        |column_names     | The source table column names you want to extract                    | Optional     |         |
|        |custom_where_sql | A custom WHERE clause to filter the source data when needed          | Optional     |         |
||
| ingest | | A Redshift internal table to copy the source data from the datalake | Required     |         |
|        | schema_name | Redshift internal schema where the ingest table exists               | Optional (*) |             |
|        | table_name | Redshift internal table to ingest the data into                      | Required     |             |

#### Notes :
 
> (*) These parameters are optional if there is an entry added into **etl.config** table for them, otherwise they are required. 




### Merge and Load (write back to data lake)
```json
{
    "target": {
        "schema_name": "....",
        "table_name": "....",
        "partition_registration": "...."
    },
    "delta": {
        "schema_name": "....",
        "table_name": "...."
    },
    "staging": {
        "schema_name": "....",
        "table_name": "....",
        "refresh_type": "...."
    },
    "upsert": {
        "type": "....",
        "keys": "...."
    }
}
```


| Key     | Sub Key                | Description                                                                                                                                                                                          | Type         | Default                   | Valid Values      |
|---------|------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------|---------------------------|-------------------|
||
| target  |                        | A data lake table where you want to load the data                                                                                                                                                    |
|         | schema_name            | Redshift external schema where the target table exists                                                                                                                                               | Optional (*) |
|         | table_name             | Redshift external table which points the target data                                                                                                                                                 | Required     |
|         | partition_registration | New data lake table partition registration method (**)                                                                                                                                               | Required     | msck  | msck, spectrum    |
||
| delta   |                        | A Redshift internal table to prepare a delta dataset to merge into staging or target table                                                                                                           |
|         | schema_name            | Redshift internal schema where the delta table exists                                                                                                                                                | Optional (*) |
|         | table_name             | Redshift internal table to store the delta data                                                                                                                                                      | Optional     | <target_table_name>_delta |
||
| staging |                        | A Redshift internal table to perform upserts using delta                                                                                                                                             |
|         | schema_name            | Redshift internal schema where the staging table exists                                                                                                                                              | Optional (*) |
|         | table_name             | Redshift internal table to ingest the data into                                                                                                                                                      | Optional     | <target_table_name>_stg   |
|         | refresh_type           | How to refresh the staging data. <br>*If value = reload, the framework reloads the data from target table partitions before performing the upsert*                                                   | Optional     | | reload            |
||
| upsert  |                        | A Redshift internal table to copy the source data from the datalake                                                                                                                                  | Required     |
|         | type                   | How to perform the merge operation<br>- pk : *Use the primary key of the table*<br>- keys : *Use the provided columns as the merge keys*<br>- all : *truncate and repopulate the staging from delta* | Optional     |pk| pk<br>all<br>keys|
|         | keys                      | List of merge column names separated by comma<br>*This works only when upsert.type = keys*                                                                                                           | Optional     |

#### Notes :
 
> (*) These parameters are optional if there is an entry added into **etl.config** table for them, otherwise they are required. 

## Config Examples for Merge and Load (write back to data lake) 

### 1- Perform UPSERT on a data lake table using delta
```
{
    "target":{
        "schema_name":"datalake",
        "table_name":"orders"
    },    
    "delta":{
        "schema_name":"etl",
        "table_name":"orders_delta"     
    }
}
```

 #### Usage Notes :
 - The framework can perform UPSERT on both non-partitioned or partitioned data lake tables.
 - For full target table refresh, just populate the delta with complete table data.
 - For partitioned tables, the new partition will be registered by running MSCK TABLE REPAIR command on Athena. This might be a heavy operation on very large tables. For very large tables consider to use Spectrum technique by adding this to your config :
 ```
 "target":{
       "partition_registration":"spectrum"
 },
 ```
 - If you don't provide parameters for staging 
   - the stating schema name could be taken from etl.config table
   - the staging table name can have default value as <target_table_name>_stg
   
### 2- Reload Staging before performing UPSERT
This could be needed in your ETL if there is another process which makes data changes in your target table

To reload the staging partitions (or full table for non-partitoned tables) add this config item :
```
 "staging":{
       "refresh_type":"reload"
 },
 ```

### 3- To perform the Merge using specified columns
If your table does not have a Primary Key you can specify the merge columns to be used during the merge (upsert)

To use specified columns in the merge add this config item :
```
 "upsert":{
       "type":"keys",
       "keys":"o_orderkey, o_orderline_number"
 },
 ```

### 4- To perform a full refresh from delta
If you don't want to use  table does not have a Primary Key you can specify the merge columns to be used during the merge (upsert)

To use specified columns in the merge add this config item :
```
 "upsert":{
       "type":"keys",
       "keys":"o_orderkey, o_orderline_number"
 },
 ```

## FAQ

### Can I use the framework if my target table is an Redshift table not a data lake table?

Yes you can. To do that ; 
- Set your target schema to internal schema
- Set your target table as an internal table
- The staging table settings will be ignored so no need to set the staging. 

Example : 
```
{
    "target":{
        "schema_name":"public",
        "table_name":"orders"
    },    
    "delta":{
        "schema_name":"etl",
        "table_name":"orders_delta"     
    }
}
```

This will merge the delta to your internal target table


### How do I call the framework stored procedures?

You need a tool to establish a connection to Redshift and execute SQL statements. This tool could be ;
- AWS Step Functions
- AWS Lambda
- Command line tools like rsql and psql
- Any orchestration or scheduling tools that can establish a connection to Redshift
- Any tools that can use Redshift Data API




## License

[MIT](https://choosealicense.com/licenses/mit/)