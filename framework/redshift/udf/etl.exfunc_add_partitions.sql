CREATE EXTERNAL FUNCTION etl.exfunc_add_partitions(varchar) 
RETURNS varchar 
STABLE 
LAMBDA 'athena-msck-table-repair' 
IAM_ROLE 'arn:aws:iam::<your_account_id>:role/redshift-udf-lambda';