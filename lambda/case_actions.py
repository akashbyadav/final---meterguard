"""Maintain budget-case state in DynamoDB.

API Gateway invokes the acknowledgement path. Step Functions invokes the
open, status, and escalate actions. Required environment: CASES_TABLE.
"""

import json
import os
import re
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError


def now():
    """Return a UTC timestamp for a case-state change."""

    return datetime.now(timezone.utc).isoformat()


def lambda_handler(event, context):
    """Handle one acknowledgement request or one workflow case action."""

    table = boto3.resource("dynamodb").Table(os.environ["CASES_TABLE"])

    # 1. API Gateway proxy requests contain httpMethod. This branch only
    #    acknowledges an existing case; it cannot invoke workflow actions.
    #    The deployed API method requires AWS_IAM authorization.
    if "httpMethod" in event:
        path_parameters = event.get("pathParameters") or {}
        case_id = path_parameters.get("case_id", "")

        if not re.fullmatch(r"[a-f0-9]{32}", case_id):
            return {
                "statusCode": 400,
                "body": '{"error":"Invalid case_id"}',
            }

        try:
            # The existence condition prevents an unknown ID from creating an
            # incomplete case. if_not_exists preserves the first acknowledgement
            # timestamp if the same HTTP request is delivered again.
            table.update_item(
                Key={"case_id": case_id},
                UpdateExpression=(
                    "SET #s = :ack, "
                    "acknowledged_at = if_not_exists(acknowledged_at, :time)"
                ),
                ConditionExpression="attribute_exists(case_id)",
                ExpressionAttributeNames={"#s": "status"},
                ExpressionAttributeValues={
                    ":ack": "ACKNOWLEDGED",
                    ":time": now(),
                },
            )

        except ClientError as error:
            if error.response["Error"]["Code"] == "ConditionalCheckFailedException":
                return {
                    "statusCode": 404,
                    "body": '{"error":"Case not found"}',
                }

            # A service or permission failure must remain an error response.
            raise

        return {
            "statusCode": 200,
            "body": json.dumps({
                "case_id": case_id,
                "status": "ACKNOWLEDGED",
            }),
        }

    # 2. Internal workflow calls carry an explicit action and the original case.
    #    The workflow role invokes Lambda directly rather than through the API.
    case = event["case"]
    case_id = case["case_id"]
    action = event["action"]

    if action == "open":
        item = dict(
            case,
            status="OPEN",
            created_at=now(),
        )

        try:
            # A repeated open must not overwrite an acknowledgement or reset
            # an escalated case to OPEN.
            table.put_item(
                Item=item,
                ConditionExpression="attribute_not_exists(case_id)",
            )

        except ClientError as error:
            if error.response["Error"]["Code"] != "ConditionalCheckFailedException":
                raise

        # Read the existing/current item consistently after the conditional
        # creation. Returning notify=False suppresses a new workflow's initial
        # notification when the logical case already reached a terminal status.
        stored = table.get_item(
            Key={"case_id": case_id},
            ConsistentRead=True,
        )["Item"]

        identity_fields = (
            "tenant_id",
            "billing_period",
            "processing_version",
            "budget_policy_version",
        )

        for key in identity_fields:
            if stored[key] != case[key]:
                raise ValueError("Existing case has different business identity")

        return {
            "notify": stored["status"] == "OPEN",
            "status": stored["status"],
        }

    if action == "status":
        # The workflow checks this after its acknowledgement wait. A missing
        # case is an error to investigate, not an implicit acknowledgement.
        stored = table.get_item(
            Key={"case_id": case_id},
            ConsistentRead=True,
        ).get("Item")

        if not stored:
            raise ValueError("Case disappeared during workflow execution")

        return {"status": stored["status"]}

    if action == "escalate":
        try:
            # Only OPEN can become ESCALATED. An acknowledgement can race the
            # notification, but this write must preserve ACKNOWLEDGED.
            table.update_item(
                Key={"case_id": case_id},
                UpdateExpression="SET #s = :escalated, escalated_at = :time",
                ConditionExpression="#s = :open",
                ExpressionAttributeNames={"#s": "status"},
                ExpressionAttributeValues={
                    ":open": "OPEN",
                    ":escalated": "ESCALATED",
                    ":time": now(),
                },
            )

        except ClientError as error:
            # A failed condition means another action already changed status.
            # Other failures still propagate so Step Functions can retry.
            if error.response["Error"]["Code"] != "ConditionalCheckFailedException":
                raise

        return {"case_id": case_id}

    raise ValueError("Unknown case action")
