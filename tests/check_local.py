"""Run offline behavioural checks with mocked AWS clients.

Run: python3 tests/check_local.py from the project directory.
These checks require boto3 locally but do not contact AWS or run Spark/Redshift.
"""

import importlib.util
import json
from datetime import datetime, timezone
from decimal import Decimal
from io import BytesIO
from pathlib import Path
from unittest.mock import Mock, patch
from urllib.error import HTTPError

from botocore.credentials import Credentials
from botocore.exceptions import ClientError

ROOT = Path(__file__).resolve().parents[1]


def load(name, path):
    """Import a standalone deployment file without adding package scaffolding."""

    spec = importlib.util.spec_from_file_location(name, ROOT / path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    return module


producer = load("producer", "lambda/producer.py")
stream = load("usage_stream", "glue/usage_stream.py")
lookup = load("context_api", "lambda/context_api.py")
starter = load("workflow_starter", "lambda/workflow_starter.py")
cases = load("case_actions", "lambda/case_actions.py")
dispatcher = load("outbox_dispatcher", "lambda/outbox_dispatcher.py")


def reference():
    """Return the historical contract/rate used by the initial event set."""

    return {
        "tenant_id": "customer-017",
        "contract_version": "v1",
        "rate_id": "document-v1",
        "service": "document_extraction",
        "currency": "INR",
        "rate_per_page": "0.40",
        "budget_amount": "500",
        "budget_policy_version": "monthly-500-v1",
        "valid_from": "2026-01-01T00:00:00Z",
        "valid_to": "2026-10-18T12:00:00Z",
    }


def usage():
    """Return the first event from the documented producer invocation."""

    return producer.make_event(
        "baseline-001",
        0,
        "2026-10-18T10:00:00Z",
        "customer-017",
        "v1",
    )


def rejects(call, error_type=ValueError):
    """Fail a check unless the supplied operation raises its expected error."""

    try:
        call()
    except error_type:
        return

    raise AssertionError(f"Expected {error_type.__name__}")


def test_stable_identity_and_precise_pricing():
    """Repeated/equivalent input must produce the same exactly priced result."""

    original = usage()
    assert original == usage()

    parsed = stream.validate_payload(json.dumps(original))
    fact = stream.make_fact(parsed, reference(), "v1")

    assert fact["estimated_charge"] == Decimal("8.000000")
    assert fact == stream.make_fact(parsed, reference(), "v1")

    # Different timezone notation for the same instant must not change hashes.
    alternate = dict(
        original,
        event_time="2026-10-18T15:30:00+05:30",
    )
    alternate_payload = stream.validate_payload(json.dumps(alternate))

    assert stream.make_fact(alternate_payload, reference(), "v1") == fact

    # A failed operation is retained as a fact but contributes zero charge.
    failed = stream.validate_payload(
        json.dumps(dict(original, outcome="FAILED"))
    )

    assert stream.make_fact(failed, reference(), "v1")["estimated_charge"] == 0


def test_invalid_payload_and_historical_rate():
    """Reject invalid usage and detect changed historical pricing."""

    invalid_inputs = (
        "not json",
        "[]",
        json.dumps(dict(usage(), pages_processed=True)),
        json.dumps(dict(usage(), pages_processed=-1)),
        json.dumps(dict(usage(), event_time="2026-10-18T10:00:00")),
    )

    for bad in invalid_inputs:
        rejects(lambda: stream.validate_payload(bad))

    parsed = stream.validate_payload(json.dumps(usage()))
    old = stream.make_fact(parsed, reference(), "v1")
    changed_reference = dict(
        reference(),
        rate_id="document-v2",
        rate_per_page="0.30",
    )
    changed = stream.make_fact(parsed, changed_reference, "v1")

    # Only the resolved result changes; the original source stays identical.
    assert old["result_hash"] != changed["result_hash"]
    assert old["source_hash"] == changed["source_hash"]

    boundary = stream.validate_payload(
        json.dumps(dict(usage(), event_time="2026-10-18T12:00:00Z"))
    )
    rejects(lambda: stream.make_fact(boundary, reference(), "v1"))

    overprecise_rate = dict(reference(), rate_per_page="0.1234567")
    rejects(
        lambda: stream.make_fact(parsed, overprecise_rate, "v1"),
        RuntimeError,
    )


def test_partition_lookup_is_batched_and_missing_is_quarantined():
    """Use one lookup per distinct key, while preserving rejects and outages."""

    rows = [{"payload": json.dumps(usage())} for _ in range(20)]
    rows.append({"payload": "malformed"})

    with patch.object(stream, "lookup_context", return_value=[reference()]) as api:
        output = list(stream.enrich_partition(
            rows,
            "https://example.invalid",
            "us-east-1",
            "v1",
        ))

        api.assert_called_once()
        assert len(api.call_args.args[0]) == 1
        assert sum(row["record_type"] == "VALID" for row in output) == 20
        assert sum(row["record_type"] == "REJECTED" for row in output) == 1

    missing = {
        "tenant_id": "customer-017",
        "contract_version": "v1",
        "error": "CONTRACT_NOT_FOUND",
    }

    with patch.object(stream, "lookup_context", return_value=[missing]):
        result = list(stream.enrich_partition(
            rows[:1],
            "https://example.invalid",
            "us-east-1",
            "v1",
        ))

        assert result[0]["reason"] == "CONTRACT_NOT_FOUND"

    # A temporary API outage must fail processing, not turn usage into rejects.
    with patch.object(
        stream,
        "lookup_context",
        side_effect=TimeoutError("outage"),
    ):
        rejects(
            lambda: list(stream.enrich_partition(
                rows[:1],
                "https://example.invalid",
                "us-east-1",
                "v1",
            )),
            TimeoutError,
        )


def test_context_http_errors_explain_rejections_without_echoing_secrets():
    """Distinguish IAM, route, and signing errors without leaking error bodies."""

    examples = (
        (
            "Missing Authentication Token",
            "MissingAuthenticationTokenException",
            "exact POST resource path",
        ),
        (
            "User is not authorized to perform execute-api:Invoke",
            "AccessDeniedException",
            "actual Glue role",
        ),
        (
            "Signature does not match. x-amz-security-token: sensitive-token",
            "InvalidSignatureException",
            "endpoint Region",
        ),
        (
            "The security token has expired",
            "ExpiredTokenException",
            "temporary credentials",
        ),
    )

    for message, error_type, expected in examples:
        error = HTTPError(
            "https://example.invalid/prod/billing-context/batch",
            403,
            "Forbidden",
            {"x-amzn-errortype": error_type, "x-amzn-requestid": "request-123"},
            BytesIO(json.dumps({"message": message}).encode()),
        )
        description = stream.describe_context_error(error)

        assert expected in description
        assert "request_id=request-123" in description
        assert "sensitive-token" not in description

    # An HTML or empty error response still produces an actionable HTTP error.
    error = HTTPError(
        "https://example.invalid",
        403,
        "Forbidden",
        {},
        BytesIO(b"<html>Forbidden</html>"),
    )
    assert "HTTP 403" in stream.describe_context_error(error)


def test_context_client_signs_requests_and_fails_fast_on_403():
    """Use temporary-role signing and surface a 403 without HTTP retry loops."""

    session = Mock()
    session.get_credentials.return_value = Credentials(
        "offline-access-key",
        "offline-secret-key",
        "offline-session-token",
    )
    error = HTTPError(
        "https://example.invalid/prod/billing-context/batch",
        403,
        "Forbidden",
        {"x-amzn-requestid": "request-456"},
        BytesIO(b'{"message":"Missing Authentication Token"}'),
    )

    with (
        patch.object(stream.time, "sleep") as sleep,
        patch("boto3.Session", return_value=session),
        patch("urllib.request.urlopen", side_effect=error) as send,
    ):
        try:
            stream.lookup_context(
                [{"tenant_id": "customer-017", "contract_version": "v1"}],
                "https://example.invalid/prod/billing-context/batch",
                "us-east-1",
            )
        except RuntimeError as failure:
            assert "request_id=request-456" in str(failure)
            assert "exact POST resource path" in str(failure)
        else:
            raise AssertionError("The rejected lookup must fail processing")

        send.assert_called_once()
        sleep.assert_not_called()
        request = send.call_args.args[0]
        headers = {name.lower(): value for name, value in request.header_items()}

        assert request.get_method() == "POST"
        assert "/us-east-1/execute-api/aws4_request" in headers["authorization"]
        assert headers["x-amz-security-token"] == "offline-session-token"


def test_dynamodb_partial_reads_are_retried():
    """Retry unprocessed keys without losing completed lookup results."""

    client = Mock()
    keys = [{"rate_id": "one"}]
    pending = {
        "rates": {
            "Keys": keys,
            "ConsistentRead": True,
        }
    }
    client.batch_get_item.side_effect = [
        {
            "Responses": {"rates": []},
            "UnprocessedKeys": pending,
        },
        {
            "Responses": {"rates": [{"rate_id": "one"}]},
            "UnprocessedKeys": {},
        },
    ]

    with patch.object(lookup.time, "sleep"):
        result = lookup.read_items(client, "rates", keys)

    assert result == [{"rate_id": "one"}]
    assert client.batch_get_item.call_count == 2
    assert client.batch_get_item.call_args.kwargs["RequestItems"] == pending


def test_producer_only_retries_failed_records():
    """A partial publication failure must not resend the successful entries."""

    client = Mock()
    records = [
        {"Data": b"one", "PartitionKey": "a"},
        {"Data": b"two", "PartitionKey": "b"},
    ]
    client.put_records.side_effect = [
        {
            "Records": [
                {"SequenceNumber": "1"},
                {"ErrorCode": "ProvisionedThroughputExceededException"},
            ]
        },
        {
            "Records": [{"SequenceNumber": "2"}]
        },
    ]

    with patch.object(producer.time, "sleep"):
        producer.publish(client, records, "stream")

    assert client.put_records.call_args.kwargs["Records"] == records[1:]


def test_documented_producer_invocation_and_repeat():
    """Check the actual console input, wire payloads, totals, and repeatability."""

    inputs = json.loads((ROOT / "config/invocations.json").read_text())
    request = inputs["producer_initial"]
    client = Mock()
    context = Mock()
    context.get_remaining_time_in_millis.return_value = 180000

    def accept_records(**request):
        """Pretend Kinesis accepted every entry without making external calls."""

        return {
            "Records": [
                {"SequenceNumber": str(index)}
                for index, _ in enumerate(request["Records"])
            ]
        }

    client.put_records.side_effect = accept_records

    with (
        patch.object(producer.boto3, "client", return_value=client),
        patch.object(producer.time, "sleep"),
        patch.dict(producer.os.environ, {"STREAM_NAME": "mg-usage-stream"}),
    ):
        result = producer.lambda_handler(request, context)

        wire_records = [
            record
            for call in client.put_records.call_args_list
            for record in call.kwargs["Records"]
        ]

        # Decode both transport layers: JSON envelope, then business payload.
        payloads = [
            json.loads(json.loads(record["Data"])["payload"])
            for record in wire_records
        ]
        distinct = {item["event_id"]: item for item in payloads}

        assert result["unique_operations"] == 100
        assert result["delivered_records"] == 110
        assert result["expected_pages"] == 2000
        assert len(wire_records) == 110
        assert len(distinct) == 100
        assert all(record["Data"].endswith(b"\n") for record in wire_records)
        assert all(
            record["PartitionKey"] == "customer-017"
            for record in wire_records
        )
        assert sum(item["pages_processed"] for item in distinct.values()) == 2000
        assert payloads[0]["event_time"] == "2026-10-18T10:00:00Z"
        assert payloads[-1]["event_time"] == "2026-10-18T10:01:39Z"

        # Retrying the exact invocation must reproduce identical bytes and IDs.
        client.put_records.reset_mock()
        repeated = producer.lambda_handler(request, context)
        repeated_records = [
            record
            for call in client.put_records.call_args_list
            for record in call.kwargs["Records"]
        ]

        assert repeated == result
        assert repeated_records == wire_records


def seeded_references():
    """Decode the exact DynamoDB items an operator enters through the console."""

    document = json.loads((ROOT / "config/reference-items.json").read_text())
    tables = {}

    for name in ("mg-contracts", "mg-rates"):
        tables[name] = [
            {field: next(iter(value.values())) for field, value in item.items()}
            for item in document[name]
        ]

    return tables


def test_sustained_reference_preflight():
    """Require all configured customer contracts and the shared rate to exist."""

    tables = seeded_references()
    database = Mock()
    contracts = Mock()
    rates = Mock()
    database.Table.side_effect = {
        "mg-contracts": contracts,
        "mg-rates": rates,
    }.__getitem__
    rates.get_item.return_value = {"Item": tables["mg-rates"][0]}

    def read_contract(**request):
        """Match the actual composite key against the supplied seed records."""

        assert request["ConsistentRead"] is True

        for item in tables["mg-contracts"]:
            if all(item[key] == value for key, value in request["Key"].items()):
                return {"Item": item}

        return {}

    contracts.get_item.side_effect = read_contract
    started = datetime(2026, 10, 18, 14, tzinfo=timezone.utc)

    with (
        patch.object(producer.boto3, "resource", return_value=database),
        patch.dict(producer.os.environ, {
            "CONTRACT_TABLE": "mg-contracts",
            "RATE_TABLE": "mg-rates",
        }),
    ):
        producer.check_reference_data(started, 900)
        assert contracts.get_item.call_count == 4

        # Bad setup fails before any Kinesis write, instead of generating a
        # large stream that the consumer would have to quarantine afterward.
        rates.get_item.return_value = {}
        rejects(lambda: producer.check_reference_data(started, 900))
        rates.get_item.return_value = {"Item": tables["mg-rates"][0]}
        tables["mg-contracts"] = tables["mg-contracts"][:2]
        rejects(lambda: producer.check_reference_data(started, 900))


def test_full_fifteen_minute_workload():
    """Simulate all 180,000 operations with virtual time and real validation."""

    inputs = json.loads((ROOT / "config/invocations.json").read_text())
    original = inputs["producer_sustained"]
    request = original
    tables = seeded_references()
    references = {}

    for contract in tables["mg-contracts"]:
        for rate in tables["mg-rates"]:
            if contract["rate_id"] == rate["rate_id"]:
                key = (contract["tenant_id"], contract["contract_version"])
                references[key] = dict(contract, **rate)

    # The virtual clock avoids waiting 15 minutes. Capturing counters rather
    # than every mocked call also keeps the test's memory use bounded.
    clock = {"seconds": 0.0}
    totals = {"operations": 0, "deliveries": 0, "failed": 0, "pages": 0}
    previous = {"id": None, "payload": None}
    observed_customers = set()
    observed_pages = set()
    context = Mock()
    context.get_remaining_time_in_millis.return_value = 600000

    def advance(seconds):
        """Advance simulated wall time for each pacing wait."""

        assert seconds >= 0
        clock["seconds"] += seconds

    def accept_and_validate(client, records, stream_name):
        """Apply the real consumer's schema and pricing checks to every event."""

        assert stream_name == "mg-usage-stream"
        assert 1 <= len(records) <= 500
        assert sum(len(record["Data"]) for record in records) < 1000000

        for record in records:
            totals["deliveries"] += 1
            payload = json.loads(json.loads(record["Data"])["payload"])
            parsed = stream.validate_payload(json.dumps(payload))
            key = (payload["tenant_id"], payload["contract_version"])
            fact = stream.make_fact(parsed, references[key], "v1")
            assert record["PartitionKey"] == payload["tenant_id"]
            assert fact["estimated_charge"] == (
                Decimal(payload["pages_processed"]) * Decimal("0.40")
            )

            if payload["event_id"] == previous["id"]:
                assert payload == previous["payload"]
                continue

            # Global sequence numbers must stay contiguous across invocations.
            sequence = totals["operations"]
            assert payload["event_id"] == (
                f"USG-{original['run_id']}-{sequence:09d}"
            )
            totals["operations"] += 1
            totals["pages"] += payload["pages_processed"]
            totals["failed"] += int(payload["outcome"] == "FAILED")
            observed_customers.add(payload["tenant_id"])
            observed_pages.add(payload["pages_processed"])
            previous.update(id=payload["event_id"], payload=payload)

    with (
        patch.object(producer.boto3, "client"),
        patch.object(producer, "check_reference_data"),
        patch.object(producer, "publish", new=accept_and_validate),
        patch.object(producer.time, "monotonic", new=lambda: clock["seconds"]),
        patch.object(producer.time, "sleep", new=advance),
        patch.dict(producer.os.environ, {"STREAM_NAME": "mg-usage-stream"}),
        patch("builtins.print"),
    ):
        segments = []

        while True:
            result = producer.lambda_handler(request, context)
            segments.append(result["segment"])

            if result["complete"]:
                break

            request = result["next_input"]

    assert len(segments) == 3
    assert clock["seconds"] == 900
    assert totals["operations"] == result["unique_operations"] == 180000
    assert totals["deliveries"] == result["planned_delivery_records"] == 181800
    assert sum(item["billable_pages"] for item in segments) == totals["pages"]
    assert 0 < totals["failed"] < 9000
    assert len(observed_customers) == 4
    assert len(observed_pages) > 20
    assert previous["payload"]["event_time"] == "2026-10-18T14:14:59.995000Z"


def test_sustained_segment_repeat_and_bad_settings():
    """Preserve bytes on retry and reject unsafe rates/invalid continuations."""

    inputs = json.loads((ROOT / "config/invocations.json").read_text())
    request = dict(inputs["producer_sustained"], duration_seconds=2)
    context = Mock()
    context.get_remaining_time_in_millis.return_value = 600000

    with (
        patch.object(producer.boto3, "client"),
        patch.object(producer, "check_reference_data"),
        patch.object(producer, "publish") as publish,
        patch.object(producer.time, "monotonic", return_value=0),
        patch.object(producer.time, "sleep"),
        patch.dict(producer.os.environ, {"STREAM_NAME": "mg-usage-stream"}),
        patch("builtins.print"),
    ):
        first_result = producer.lambda_handler(request, context)
        first_calls = list(publish.call_args_list)
        publish.reset_mock()
        repeated_result = producer.lambda_handler(request, context)

        assert first_result == repeated_result
        assert first_calls == publish.call_args_list

        for changes in (
            {"events_per_second": 251},
            {"events_per_second": True},
            {"duration_seconds": 0},
            {"start_second": 1},
            {"run_id": "invalid run name"},
        ):
            invalid = dict(request, **changes)
            rejects(lambda: producer.lambda_handler(invalid, context))

        publish.reset_mock()
        context.get_remaining_time_in_millis.return_value = 59000
        rejects(lambda: producer.lambda_handler(request, context), RuntimeError)
        publish.assert_not_called()


def test_workflow_retries_do_not_hide_failed_executions():
    """A repeated message can accept success, but never conceal failed work."""

    client = Mock()
    body = {
        "case_id": "a" * 32,
        "tenant_id": "customer-017",
    }
    arn = (
        "arn:aws:states:us-east-1:123456789012:"
        "stateMachine:mg-budget-workflow"
    )
    client.start_execution.side_effect = ClientError(
        {"Error": {"Code": "ExecutionAlreadyExists"}},
        "StartExecution",
    )
    client.describe_execution.return_value = {
        "input": json.dumps(body),
        "status": "SUCCEEDED",
    }

    result = starter.start_case(client, arn, body)
    assert result["executionArn"].endswith("a" * 32)

    client.describe_execution.return_value["status"] = "FAILED"
    rejects(lambda: starter.start_case(client, arn, body), RuntimeError)

    # Reusing a case ID for different content is a separate identity conflict.
    client.describe_execution.return_value = {
        "input": json.dumps(dict(body, tenant_id="other")),
        "status": "RUNNING",
    }
    rejects(lambda: starter.start_case(client, arn, body))


def test_acknowledgement_is_conditional():
    """Acknowledgement requires an existing case and preserves its first time."""

    table = Mock()
    event = {
        "httpMethod": "POST",
        "pathParameters": {"case_id": "a" * 32},
    }

    with (
        patch.object(cases.boto3, "resource") as resource,
        patch.dict(cases.os.environ, {"CASES_TABLE": "cases"}),
    ):
        resource.return_value.Table.return_value = table
        response = cases.lambda_handler(event, None)

    call = table.update_item.call_args.kwargs
    assert response["statusCode"] == 200
    assert call["ConditionExpression"] == "attribute_exists(case_id)"
    assert "if_not_exists(acknowledged_at" in call["UpdateExpression"]


def test_dispatcher_marks_only_successful_sends():
    """A failed queue entry stays pending even if another entry succeeded."""

    data = Mock()
    queue = Mock()
    data.get_statement_result.return_value = {
        "ColumnMetadata": [
            {"name": "case_id"},
            {"name": "tenant_id"},
        ],
        "Records": [
            [{"stringValue": "a" * 32}, {"stringValue": "customer-017"}],
            [{"stringValue": "b" * 32}, {"stringValue": "customer-018"}],
        ],
    }
    queue.send_message_batch.return_value = {
        "Successful": [{"Id": "0"}],
        "Failed": [{"Id": "1", "Code": "ServiceUnavailable"}],
    }

    with (
        patch.object(dispatcher.boto3, "client", side_effect=[data, queue]),
        patch.object(dispatcher, "execute", return_value="statement") as execute,
        patch.dict(
            dispatcher.os.environ,
            {"QUEUE_URL": "https://example.invalid/queue"},
        ),
    ):
        rejects(lambda: dispatcher.lambda_handler({}, None), RuntimeError)

    assert execute.call_count == 2
    assert execute.call_args.args[2] == [{"name": "id0", "value": "a" * 32}]
    assert "WHERE sent_at IS NULL" in execute.call_args.args[1]


def test_dispatcher_never_marks_a_failed_send():
    """An uncertain SQS result must not mark notification intent as delivered."""

    data = Mock()
    queue = Mock()
    data.get_statement_result.return_value = {
        "ColumnMetadata": [{"name": "case_id"}],
        "Records": [[{"stringValue": "a" * 32}]],
    }
    queue.send_message_batch.side_effect = TimeoutError("outcome uncertain")

    with (
        patch.object(dispatcher.boto3, "client", side_effect=[data, queue]),
        patch.object(dispatcher, "execute", return_value="statement") as execute,
        patch.dict(
            dispatcher.os.environ,
            {"QUEUE_URL": "https://example.invalid/queue"},
        ),
    ):
        rejects(lambda: dispatcher.lambda_handler({}, None), TimeoutError)

    # Only the initial SELECT happened; no sent_at UPDATE was attempted.
    assert execute.call_count == 1


def test_queue_handler_returns_only_failed_message_ids():
    """A malformed message must not cause its valid neighbour to be retried."""

    records = [
        {
            "messageId": "good",
            "body": json.dumps({"case_id": "a" * 32}),
        },
        {
            "messageId": "bad",
            "body": "malformed",
        },
    ]

    with (
        patch.object(starter.boto3, "client"),
        patch.object(starter, "start_case") as start,
        patch.dict(starter.os.environ, {"STATE_MACHINE_ARN": "state-machine"}),
        patch("builtins.print"),
    ):
        result = starter.lambda_handler({"Records": records}, None)

    assert result == {"batchItemFailures": [{"itemIdentifier": "bad"}]}
    start.assert_called_once()


def test_configuration_and_schema_contracts():
    """Check configuration syntax, workflow targets, and the fact-column order."""

    for file in (ROOT / "config").glob("*.json"):
        json.loads(file.read_text())

    for file in (ROOT / "config").glob("*-workflow.asl.json"):
        workflow = json.loads(file.read_text())
        states = workflow["States"]
        assert workflow["StartAt"] in states

        for state in states.values():
            assert state["Comment"]

            for name in (state.get("Next"), state.get("Default")):
                assert name is None or name in states

            for choice in state.get("Choices", []):
                assert choice["Next"] in states

    # The producer must wait for a segment result before using its next offset.
    # A pinned function version protects deterministic retries from code edits.
    path = ROOT / "config/producer-workflow.asl.json"
    producer_states = json.loads(path.read_text())["States"]
    task = producer_states["ProduceSegment"]
    assert task["OutputPath"] == "$.Payload"
    assert task["Parameters"]["Payload.$"] == "$"
    assert task["Parameters"]["FunctionName"].endswith(":PRODUCER_VERSION")
    assert task["Parameters"].get("InvocationType", "RequestResponse") == (
        "RequestResponse"
    )
    assert producer_states["UseNextSegmentInput"]["InputPath"] == "$.next_input"

    # Ignore explanatory SQL comments when reading the declared column order.
    # This check catches schema drift; it does not execute SQL in Redshift.
    sql = (ROOT / "sql/schema.sql").read_text()
    table = sql.split("CREATE TABLE IF NOT EXISTS mg.usage_fact (", 1)[1]
    table = table.split(") DISTSTYLE", 1)[0]
    columns = [
        line.strip().split()[0]
        for line in table.splitlines()
        if line.strip() and not line.lstrip().startswith("--")
    ]

    assert columns == stream.FACT_COLUMNS


if __name__ == "__main__":
    # Discover these plain functions without requiring a test-runner package.
    tests = [
        value
        for name, value in list(globals().items())
        if name.startswith("test_")
    ]

    for test in tests:
        test()
        print(f"PASS {test.__name__}")

    print(
        f"{len(tests)} offline checks passed; "
        "cloud acceptance steps are in runbook.md"
    )
