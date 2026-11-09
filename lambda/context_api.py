"""Return versioned contracts and rates for Glue's batched lookup request.

API Gateway invokes lambda_handler using Lambda proxy integration.
Required environment variables: CONTRACTS_TABLE and RATES_TABLE.
"""

import base64
import json
import os
import re
import time
from decimal import Decimal

import boto3


def read_items(client, table, keys):
    """Read the requested DynamoDB items, retrying unfinished keys separately."""

    # Strong reads prevent a freshly created reference item from appearing
    # missing. The caller limits each request to at most 50 distinct keys.
    pending = {
        table: {
            "Keys": keys,
            "ConsistentRead": True,
        }
    }
    items = []

    for attempt in range(5):
        response = client.batch_get_item(RequestItems=pending)
        items.extend(response.get("Responses", {}).get(table, []))

        # Unprocessed keys mean DynamoDB did not finish the read. They are not
        # evidence that the requested contract or rate does not exist.
        pending = response.get("UnprocessedKeys", {})

        if not pending:
            return items

        time.sleep(min(0.2 * 2 ** attempt, 2))

    # Fail the API request after bounded retries. Glue must retry processing
    # rather than quarantine valid usage because of a temporary service outage.
    raise RuntimeError("DynamoDB returned unprocessed lookup keys after retries")


def response(status, body):
    """Build the HTTP response structure expected by API Gateway."""

    return {
        "statusCode": status,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(body, default=str),
    }


def lambda_handler(event, context):
    """Validate lookup keys, read historical references, and return one per key."""

    # 1. Decode the proxy request. API Gateway can deliver the body either as
    #    ordinary text or as base64 text, depending on its integration settings.
    try:
        raw = event.get("body") or "{}"

        if event.get("isBase64Encoded"):
            raw = base64.b64decode(raw).decode()

        body = json.loads(raw)
        keys = body["keys"]

        if not isinstance(keys, list) or not 1 <= len(keys) <= 50:
            raise ValueError("keys must contain 1..50 contract identifiers")

        for key in keys:
            for field in ("tenant_id", "contract_version"):
                if not re.fullmatch(r"[A-Za-z0-9_-]{1,64}", key[field]):
                    raise ValueError(f"Invalid {field}")

    except (ValueError, KeyError, TypeError):
        return response(
            400,
            {"error": "Expected keys with tenant_id and contract_version"},
        )

    # 2. Read each customer/version pair once, even if multiple usage events
    #    requested it. The resource API accepts ordinary Python key values.
    client = boto3.resource("dynamodb")
    distinct = sorted({
        (key["tenant_id"], key["contract_version"])
        for key in keys
    })
    contract_keys = [
        {"tenant_id": tenant, "contract_version": version}
        for tenant, version in distinct
    ]
    contracts = read_items(
        client,
        os.environ["CONTRACTS_TABLE"],
        contract_keys,
    )

    # 3. A contract points to an immutable rate ID. Read those exact rates;
    #    looking up a customer's latest price would change historical results.
    rate_ids = sorted({item["rate_id"] for item in contracts})
    rates = []

    if rate_ids:
        rate_keys = [{"rate_id": value} for value in rate_ids]
        rates = read_items(client, os.environ["RATES_TABLE"], rate_keys)

    # Key the results for direct lookup. DynamoDB does not return BatchGetItem
    # results in the same order as the requested keys.
    contracts = {
        (item["tenant_id"], item["contract_version"]): item
        for item in contracts
    }
    rates = {item["rate_id"]: item for item in rates}

    # 4. Return one context for every requested pair. Missing reference data is
    #    an explicit business rejection; an AWS read failure propagates instead.
    results = []

    for tenant, version in distinct:
        contract = contracts.get((tenant, version))
        rate = None

        if contract:
            rate = rates.get(contract["rate_id"])

        result = {
            "tenant_id": tenant,
            "contract_version": version,
        }

        if not contract:
            result["error"] = "CONTRACT_NOT_FOUND"
        elif not rate:
            result["error"] = "RATE_NOT_FOUND"
        else:
            # Copy only the fields required to validate and price a usage event.
            result.update({
                "valid_from": contract["valid_from"],
                "valid_to": contract["valid_to"],
                "budget_amount": contract["budget_amount"],
                "budget_policy_version": contract["budget_policy_version"],
                "rate_id": rate["rate_id"],
                "service": rate["service"],
                "currency": rate["currency"],
                "rate_per_page": rate["rate_per_page"],
            })

            # Decimal strings cross the JSON boundary without floating-point
            # rounding. Glue converts them back into Decimal before calculating.
            for name in ("budget_amount", "rate_per_page"):
                result[name] = str(Decimal(str(result[name])))

        results.append(result)

    return response(200, {"contexts": results})
