-- Create ETL schema
create schema if not exists etl;

-- Create an external schema in Redshift
drop schema datalake;
create external schema datalake
from data catalog
database 'default'
region 'us-west-2' 
iam_role 'arn:aws:iam::<your_account_number>:role/<your_iam_role>';

------- External Tables ------------------
-- Create external Source table
drop table if exists datalake.orders_src;
create external table datalake.orders_src
    (jsondata varchar(10000))
stored as textfile
location 's3://<your_s3_bucket>/tpch/orders_src/';

-- Create external Target table
drop table if exists datalake.orders;
create external table datalake.orders (
    o_orderkey      bigint,
    o_custkey       bigint,
    c_name          varchar(25),
    c_nationname    char(25),
    o_orderstatus   char(1),
    o_totalprice    decimal(12,2),
    o_orderdate     date,
    o_orderpriority varchar(15),
    o_clerk         varchar(15),
    o_shippriority  int,
    o_comment       varchar(100)
    )
partitioned by (order_year int, order_month int)
stored as parquet
location 's3://<your_s3_bucket>/tpch/orders';


------- Internal Tables ------------------
-- Create internal Source table
drop table if exists etl.orders_src;
create table etl.orders_src (like datalake.orders_src);

-- Create internal Delta table
drop table if exists etl.orders_delta;
create table etl.orders_delta (like datalake.orders);


-- Create internal Staging table
create table etl.orders_stg (like datalake.orders);

-- Set a Primary Key on the Staging table
alter table etl.orders_stg alter column o_orderkey type bigint not null;
alter table etl.orders_stg add primary key (o_orderkey);