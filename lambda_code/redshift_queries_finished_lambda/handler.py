import os

import boto3

dynamodb_table = boto3.resource("dynamodb").Table(os.environ["DYNAMODB_TABLE"])
sfn_client = boto3.client("stepfunctions")


def lambda_handler(event, context):
    # print("event", event)
    redshift_queries_state = event["detail"]["state"]
    if redshift_queries_state in ["SUBMITTED", "PICKED", "STARTED"]:
        print(f"Redshift state is {redshift_queries_state}, so ignore")
        return
    redshift_queries_id = event["detail"]["statementId"]
    response = dynamodb_table.get_item(Key={"redshift_queries_id": redshift_queries_id})
    if not "Item" in response:
        print(
            f'Did not find record with `redshift_queries_id` "{redshift_queries_id}" '
            f'in DynamoDB "{dynamodb_table.table_name}"'
        )
        return  # could be an error and raise exception
    record = response["Item"]
    task_token = record["task_token"]
    if redshift_queries_state == "FINISHED":
        sfn_client.send_task_success(
            taskToken=task_token,
            output='"json output of the task"',  # figure out what to write here such as completed statementId, table name
        )
        modified_record = record.copy()
        modified_record["redshift_queries_id"] = (
            "_already_processed_" + modified_record["redshift_queries_id"]
        )
        dynamodb_table.put_item(Item=modified_record)
        dynamodb_table.delete_item(Key={"redshift_queries_id": redshift_queries_id})
    elif redshift_queries_state in ["ABORTED", "FAILED"]:
        sfn_client.send_task_failure(
            taskToken=task_token,
            error='"string"',  # figure out what to write here
            cause='"string"',  # figure out what to write here
        )
    else:  # 'ALL'
        sfn_client.send_task_failure(
            taskToken=task_token,
            error=f'"`redshift_queries_state` is {redshift_queries_state}"',  # figure out what to write here
            cause='"string"',  # figure out what to write here
        )
