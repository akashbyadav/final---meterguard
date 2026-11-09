"""Start a named Standard Step Functions execution for each queued case.

Lambda's SQS trigger invokes lambda_handler. Required environment:
STATE_MACHINE_ARN. Enable ReportBatchItemFailures on the trigger.
"""

import json
import os
import re

import boto3
from botocore.exceptions import ClientError


def start_case(client, state_machine_arn, body):
    """Start a case, or verify that a repeated delivery matches existing work."""

    case_id = body["case_id"]

    if not re.fullmatch(r"[a-f0-9]{32}", case_id):
        raise ValueError(
            "case_id must be the warehouse-generated 32-character identifier"
        )

    # Standard workflow idempotency depends on both execution name and input.
    # Stable JSON formatting keeps repeated deliveries identical. Timestamps
    # or queue-receive metadata must not be added to this input.
    payload = json.dumps(
        body,
        sort_keys=True,
        separators=(",", ":"),
    )

    try:
        return client.start_execution(
            stateMachineArn=state_machine_arn,
            name=case_id,
            input=payload,
        )

    except ClientError as error:
        # Permission errors and service failures need normal queue retries.
        # Only the existing-name response enters the duplicate check below.
        if error.response["Error"]["Code"] != "ExecutionAlreadyExists":
            raise

        execution_prefix = state_machine_arn.replace(
            ":stateMachine:",
            ":execution:",
        )
        execution_arn = execution_prefix + ":" + case_id
        existing = client.describe_execution(executionArn=execution_arn)

        # The same ID with different input is a conflict, not a safe duplicate.
        if json.loads(existing["input"]) != body:
            raise ValueError("A case ID was reused with different workflow input")

        # Do not acknowledge a failed workflow as completed. Repair and redrive
        # that execution before redriving this queue message.
        if existing["status"] not in ("RUNNING", "SUCCEEDED"):
            raise RuntimeError(
                f"Existing workflow needs recovery: {existing['status']}"
            )

        return {"executionArn": execution_arn}


def lambda_handler(event, context):
    """Process queue records and return the IDs that Lambda should retry."""

    client = boto3.client("stepfunctions")
    failures = []

    for record in event["Records"]:
        try:
            body = json.loads(record["body"])
            start_case(
                client,
                os.environ["STATE_MACHINE_ARN"],
                body,
            )

        except Exception as error:
            # Handle records independently. Returning only failed message IDs
            # prevents a valid message from being retried because its neighbour
            # failed. No usage payload is written into this routine log.
            print(json.dumps({
                "message_id": record["messageId"],
                "error": str(error),
            }))
            failures.append({"itemIdentifier": record["messageId"]})

    # This response only controls retries when ReportBatchItemFailures is
    # enabled on the SQS event source mapping.
    return {"batchItemFailures": failures}
