import json
import os
import time
from pprint import pprint
from time import sleep
from typing import Callable, TypedDict, TypeVar

import psycopg2
from psycopg2.extras import RealDictCursor
from boto3 import client

from lib import query as q

STACK_NAME = os.getenv("STACK_NAME", "DMSPostgresKinesis")

ENDPOINT_URL = os.getenv("ENDPOINT_URL")

cfn = client("cloudformation", endpoint_url=ENDPOINT_URL)
dms = client("dms", endpoint_url=ENDPOINT_URL)
kinesis = client("kinesis", endpoint_url=ENDPOINT_URL)
secretsmanager = client("secretsmanager", endpoint_url=ENDPOINT_URL)


retries = 100 if not ENDPOINT_URL else 10
retry_sleep = 5 if not ENDPOINT_URL else 1


class CfnOutput(TypedDict):
    cdcTask: str
    kinesisStream: str
    dbSecret: str


class Credentials(TypedDict):
    host: str
    dbname: str
    username: str
    password: str
    port: int


def get_cfn_output():
    stacks = cfn.describe_stacks()["Stacks"]
    stack = None
    for s in stacks:
        if s["StackName"] == STACK_NAME:
            stack = s
            break
    if not stack:
        raise Exception(f"Stack {STACK_NAME} Not found")

    outputs = stack["Outputs"]
    cfn_output = CfnOutput()
    for output in outputs:
        cfn_output[output["OutputKey"]] = output["OutputValue"]
    return cfn_output


def get_credentials(secret_arn: str) -> Credentials:
    secret_value = secretsmanager.get_secret_value(SecretId=secret_arn)
    credentials = Credentials(**json.loads(secret_value["SecretString"]))
    if credentials["host"] == "postgres_server":
        credentials["host"] = "localhost"
    return credentials


T = TypeVar("T")


def retry(
    function: Callable[..., T], retries=retries, sleep=retry_sleep, **kwargs
) -> T:
    raise_error = None
    retries = int(retries)
    for i in range(0, retries + 1):
        try:
            return function(**kwargs)
        except Exception as error:
            raise_error = error
            time.sleep(sleep)
    raise raise_error


def run_queries_on_postgres(
    credentials: Credentials,
    queries: list[str],
):
    cursor = None
    cnx = None
    try:
        cnx = psycopg2.connect(
            user=credentials["username"],
            password=credentials["password"],
            host=credentials["host"],
            dbname=credentials["dbname"],
            cursor_factory=RealDictCursor,
            port=int(credentials["port"]),
        )
        cursor = cnx.cursor()
        for query in queries:
            cursor.execute(query)
        cnx.commit()
    finally:
        if cursor:
            cursor.close()
        if cnx:
            cnx.close()


def get_query_result(
    credentials: Credentials,
    query: str,
):
    cursor = None
    cnx = None
    try:
        cnx = psycopg2.connect(
            user=credentials["username"],
            password=credentials["password"],
            host=credentials["host"],
            dbname=credentials["dbname"],
            cursor_factory=RealDictCursor,
            port=int(credentials["port"]),
        )
        cursor = cnx.cursor()
        cursor.execute(query)
        return cursor.fetchall()
    finally:
        if cursor:
            cursor.close()
        if cnx:
            cnx.close()

def start_task(task: str):
    response = dms.start_replication_task(
        ReplicationTaskArn=task, StartReplicationTaskType="start-replication"
    )
    status = response["ReplicationTask"].get("Status")
    print(f"Replication Task {task} status: {status}")


def stop_task(task: str):
    response = dms.stop_replication_task(ReplicationTaskArn=task)
    status = response["ReplicationTask"].get("Status")
    print(f"\n Replication Task {task} status: {status}")


def wait_for_task_status(task: str, expected_status: str):
    print(f"Waiting for task status {expected_status}")

    def _wait_for_status():
        status = dms.describe_replication_tasks(
            Filters=[{"Name": "replication-task-arn", "Values": [task]}],
            WithoutSettings=True,
        )["ReplicationTasks"][0].get("Status")
        print(f"{task=} {status=}")
        assert status == expected_status

    retry(_wait_for_status)


def wait_for_kinesis(stream: str, expected_count: int, threshold_timestamp: int):
    print("\n\tKinesis events\n")
    print("fetching Kinesis event")

    shard_id = kinesis.describe_stream(StreamARN=stream)["StreamDescription"]["Shards"][
        0
    ]["ShardId"]
    shard_iterator = kinesis.get_shard_iterator(
        StreamARN=stream,
        ShardId=shard_id,
        ShardIteratorType="TRIM_HORIZON",
    )
    shard_iter = shard_iterator["ShardIterator"]
    all_records = []
    while shard_iter is not None:
        res = kinesis.get_records(ShardIterator=shard_iter, Limit=50)
        shard_iter = res["NextShardIterator"]
        records = res["Records"]
        for r in records:
            if r["ApproximateArrivalTimestamp"].timestamp() > threshold_timestamp:
                all_records.append(r)
        if len(all_records) >= expected_count:
            break
        print(f"found {len(all_records)}, {expected_count=}")
        sleep(retry_sleep)
    print(f"Received: {len(all_records)} events")
    pprint(
        [
            {**json.loads(record["Data"]), "partition_key": record["PartitionKey"]}
            for record in all_records
        ]
    )


def describe_table_statistics(task_arn: str):
    res = dms.describe_table_statistics(
        ReplicationTaskArn=task_arn,
    )
    res["TableStatistics"] = sorted(
        res["TableStatistics"], key=lambda x: (x["SchemaName"], x["TableName"])
    )
    return res


def execute_cdc(cfn_output: CfnOutput):
    # CDC Flow
    credentials = get_credentials(cfn_output["dbSecret"])
    task = cfn_output["cdcTask"]
    stream = cfn_output["kinesisStream"]
    print("")
    print("*" * 12)
    print("STARTING CDC FLOW")
    print("*" * 12)
    print(f"db endpoint: {credentials['host']}:{credentials['port']}\n")

    run_queries_on_postgres(credentials, q.DROP_TABLES)
    print("\tCreating tables")
    run_queries_on_postgres(credentials, q.CREATE_TABLES)

    threshold_timestamp = int(time.time())
    print("Starting CDC task")
    start_task(task)
    wait_for_task_status(task, "running")

    print("\n****Create table events****\n")
    # 1 create apply_dms_exception, 3 create
    wait_for_kinesis(stream, 4, threshold_timestamp)
    print("\n****End create table events****\n")

    print("\n****INSERT events****\n")
    sleep(1)
    threshold_timestamp = int(time.time())
    sleep(1)
    run_queries_on_postgres(credentials, q.PRESEED_DATA)
    # 1 authors, 1 accounts, 1 books
    wait_for_kinesis(stream, 3, threshold_timestamp)
    print("\n****End of INSERT events****\n")

    print("\n****ALTER tables events****\n")
    sleep(1)
    threshold_timestamp = int(time.time())
    sleep(1)
    run_queries_on_postgres(credentials, q.ALTER_TABLES)
    wait_for_kinesis(stream, 3, threshold_timestamp)
    print("\n****End of ALTER tables events****\n")

    print("\n****Table Statistics****\n")
    print("\tTable Statistics tasks")
    pprint(describe_table_statistics(task))

    stop_task(task)
    wait_for_task_status(task, "stopped")

    print("\n\tDrop tables")
    run_queries_on_postgres(credentials, q.DROP_TABLES)


if __name__ == "__main__":
    cfn_output = get_cfn_output()

    execute_cdc(cfn_output)
