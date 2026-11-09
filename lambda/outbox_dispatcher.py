"""Send committed alert-outbox rows from Redshift to SQS.

EventBridge invokes lambda_handler periodically. Required environment:
WORKGROUP_NAME, QUEUE_URL, and optionally DATABASE (default: dev).
"""

import json
import os
import time

import boto3


def execute(client, sql, parameters=None):
    """Submit one SQL statement and wait until its outcome is known."""

    request = {
        "WorkgroupName": os.environ["WORKGROUP_NAME"],
        "Database": os.environ.get("DATABASE", "dev"),
        "Sql": sql,
    }

    if parameters:
        request["Parameters"] = parameters

    # The Data API returns a statement ID before SQL execution finishes.
    # Do not treat submission as a successful database commit.
    statement_id = client.execute_statement(**request)["Id"]
    deadline = time.monotonic() + 100

    while time.monotonic() < deadline:
        status = client.describe_statement(Id=statement_id)

        if status["Status"] == "FINISHED":
            return statement_id

        if status["Status"] in ("FAILED", "ABORTED"):
            raise RuntimeError(status.get("Error", status["Status"]))

        time.sleep(1)

    # Cancellation can race a commit. Leave the outcome uncertain and fail
    # this invocation. Retrying is safe because case IDs remain unchanged.
    client.cancel_statement(Id=statement_id)
    raise TimeoutError(f"Redshift statement exceeded deadline: {statement_id}")


def lambda_handler(event, context):
    """Read pending alerts, publish them, then mark only successful sends."""

    data = boto3.client("redshift-data")

    # 1. Read at most ten committed alerts, matching one SQS batch. Casting
    #    amounts to text preserves their precision in the outgoing JSON.
    pending_sql = """
        SELECT
            case_id,
            tenant_id,
            billing_period::varchar AS billing_period,
            currency,
            estimated_total::varchar AS estimated_total,
            budget_amount::varchar AS budget_amount,
            processing_version,
            budget_policy_version
        FROM mg.alert_outbox
        WHERE sent_at IS NULL
        ORDER BY created_at, case_id
        LIMIT 10
    """
    statement_id = execute(data, pending_sql)
    result = data.get_statement_result(Id=statement_id)

    # 2. The Data API returns typed cells such as {"stringValue": "INR"}.
    #    Pair each unwrapped value with its column name to form a case payload.
    columns = [column["name"] for column in result["ColumnMetadata"]]
    rows = []

    for record in result["Records"]:
        values = [next(iter(field.values())) for field in record]
        rows.append(dict(zip(columns, values)))

    if not rows:
        return {"sent": 0}

    # 3. SQS requires an ID for each entry in this send request. That local ID
    #    lets us map individual send results back to the stable case_id.
    entries = []

    for index, row in enumerate(rows):
        entries.append({
            "Id": str(index),
            "MessageBody": json.dumps(
                row,
                sort_keys=True,
                separators=(",", ":"),
            ),
        })

    queue = boto3.client("sqs")
    sent = queue.send_message_batch(
        QueueUrl=os.environ["QUEUE_URL"],
        Entries=entries,
    )

    successful = [
        rows[int(item["Id"])]["case_id"]
        for item in sent.get("Successful", [])
    ]

    # 4. Update only alerts accepted by SQS. Updating before sending would lose
    #    an alert if publication failed. Sending before updating can duplicate a
    #    delivery after a crash; the workflow uses case_id to handle that repeat.
    if successful:
        parameters = [
            {"name": f"id{index}", "value": case_id}
            for index, case_id in enumerate(successful)
        ]
        placeholders = ",".join(
            f":id{index}"
            for index in range(len(parameters))
        )

        # Only generated parameter names enter this SQL string. Actual case IDs
        # are passed separately through Data API parameters.
        mark_sent_sql = f"""
            UPDATE mg.alert_outbox
            SET sent_at = GETDATE()
            WHERE sent_at IS NULL
                AND case_id IN ({placeholders})
        """
        execute(data, mark_sent_sql, parameters)

    # A partial SQS failure keeps unsuccessful rows pending. Raising an error
    # also makes the failure visible while the next schedule retries those rows.
    if sent.get("Failed"):
        raise RuntimeError(f"SQS rejected {len(sent['Failed'])} outbox messages")

    return {"sent": len(successful)}
