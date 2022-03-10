drop table if exists tmp1;
create temp table tmp1
as
select json_extract_path_text(jsondata,'o_orderkey')::bigint as o_orderkey,
	   to_date(json_extract_path_text(jsondata,'o_orderdate'),'YYYY-MM-DD') as o_orderdate,
	   json_extract_path_text(jsondata,'o_custkey')::bigint as o_custkey,
       json_extract_path_text(jsondata,'o_orderstatus') as o_orderstatus,
       json_extract_path_text(jsondata,'o_totalprice')::decimal(12,2) as o_totalprice,
       json_extract_path_text(jsondata,'o_orderpriority') as o_orderpriority,
       json_extract_path_text(jsondata,'o_clerk') as o_clerk,
       json_extract_path_text(jsondata,'o_shippriority')::int as o_shippriority,
       json_extract_path_text(jsondata,'o_comment') as o_comment
from etl.orders_src;

truncate table etl.orders_delta;

insert into etl.orders_delta (
	   o_orderkey,
	   o_custkey,
       c_name,
       c_nationname,
       o_orderstatus,
       o_totalprice,
       o_orderdate,
       order_year,
       order_month,
       o_orderpriority,
       o_clerk,
       o_shippriority,
       o_comment)
       
select o.o_orderkey,
	   o.o_custkey,
       c.c_name,
       n.n_name as c_nationname,
       o.o_orderstatus,
       o.o_totalprice,
       o.o_orderdate,
       cast(date_part(year,o.o_orderdate) as int) as order_year,
       cast(date_part(month,o.o_orderdate) as int) as order_month,
       o.o_orderpriority,
       o.o_clerk,
       o.o_shippriority,
       o.o_comment
from tmp1 o
join sample_data_dev.tpch.customer c
	on c.c_custkey = o.o_custkey
join sample_data_dev.tpch.nation n
	on n.n_nationkey = c.c_nationkey;