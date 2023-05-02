import json
import os
from datetime import datetime, timedelta

import boto3


dynamodb_table = boto3.resource("dynamodb").Table(os.environ["DYNAMODB_TABLE"])
redshift_data_client = boto3.client("redshift-data")
# aws_redshift.CfnCluster(...).attr_id (for cluster name) is broken, so using endpoint address instead
REDSHIFT_CLUSTER_NAME = os.environ["REDSHIFT_ENDPOINT_ADDRESS"].split(".")[0]
REDSHIFT_USER = os.environ["REDSHIFT_USER"]
REDSHIFT_DATABASE_NAME = os.environ["REDSHIFT_DATABASE_NAME"]
REDSHIFT_SCHEMA_NAME = os.environ["REDSHIFT_SCHEMA_NAME"]
REDSHIFT_TABLE_NAME = os.environ["REDSHIFT_TABLE_NAME"]
S3_FILENAME = os.environ["S3_FILENAME"]
REDSHIFT_ROLE = os.environ["REDSHIFT_ROLE"]


def lambda_handler(event, context) -> None:
    # print(f"event: {event}")
    assert (
        "task_token" in event
    ), f'"task_token" key must be in `event`. `event` is: {event}'
    sql_queries = [
        f"truncate {REDSHIFT_DATABASE_NAME}.{REDSHIFT_SCHEMA_NAME}.{REDSHIFT_TABLE_NAME};",
        f"""
        copy {REDSHIFT_DATABASE_NAME}.{REDSHIFT_SCHEMA_NAME}.{REDSHIFT_TABLE_NAME}
        from '{S3_FILENAME}'
        iam_role '{REDSHIFT_ROLE}'
        format as json 'auto';
        """,  # eventually put REDSHIFT_DATABASE_NAME, REDSHIFT_SCHEMA_NAME, REDSHIFT_TABLE_NAME as event payload
    ]
    response = redshift_data_client.batch_execute_statement(
        ClusterIdentifier=REDSHIFT_CLUSTER_NAME,
        Database=REDSHIFT_DATABASE_NAME,
        DbUser=REDSHIFT_USER,
        Sqls=sql_queries,
        WithEvent=True,
    )
    # print(response)
    response.pop(
        "CreatedAt", None
    )  # has a datetime() object that is not JSON serializable
    utc_now = datetime.utcnow()
    records_expires_on = utc_now + timedelta(days=7)
    dynamodb_table.put_item(
        Item={
            "redshift_queries_id": response["Id"],
            "task_token": event["task_token"],
            "sql_queries": json.dumps(sql_queries),
            "redshift_response": json.dumps(response),
            "utc_now_human_readable": utc_now.strftime("%Y-%m-%d %H:%M:%S") + " UTC",
            "delete_record_on": int(records_expires_on.timestamp()),
            "delete_record_on_human_readable": records_expires_on.strftime(
                "%Y-%m-%d %H:%M:%S" + " UTC"
            ),
        }
    )
