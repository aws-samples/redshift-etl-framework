import json
import boto3

def lambda_handler(event, context):
    
    for argument in event['arguments']:
        tablename = argument[0]
        
    bucket_name = '<your_bucket_name_and_path_for_athena_results>'
    client = boto3.client('athena')
    config = {
        'OutputLocation': 's3://' + bucket_name + '/',
        'EncryptionConfiguration': {'EncryptionOption': 'SSE_S3'}
    }
    
    # Query Execution Parameters
    sql = 'MSCK REPAIR TABLE default.' + tablename
    context = {'Database': 'default'}

    client.start_query_execution(QueryString = sql, 
                                 QueryExecutionContext = context,
                                 ResultConfiguration = config)
    
    res = []
    res.append(json.dumps(1))
    ret = dict()
    ret['success'] = True
    ret['results'] = res
    ret_json = json.dumps(ret)
    return ret_json
