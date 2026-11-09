"""Generate reproducible document-usage events and publish them to Kinesis.

Use the producer workflow for sustained traffic or Lambda for small count runs.
Input examples: config/invocations.json. Required environment: STREAM_NAME;
sustained traffic also requires CONTRACT_TABLE and RATE_TABLE.
This function represents the event-emitting part of a document service; it
does not read documents, call an extraction model, or calculate customer prices.
"""

import hashlib
import json
import os
import re
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import boto3
from botocore.config import Config


# These are fictional business customers with matching DynamoDB seed records.
# Page ranges reflect their document categories; they are workload assumptions,
# not measurements from real customers. Keep this catalogue unchanged on retry.
CUSTOMERS = [
    {
        "tenant_id": "aster-lending",
        "document_code": "APPLICATION",
        "minimum_pages": 8,
        "maximum_pages": 45,
    },
    {
        "tenant_id": "harbor-claims",
        "document_code": "CLAIM",
        "minimum_pages": 3,
        "maximum_pages": 24,
    },
    {
        "tenant_id": "cobalt-logistics",
        "document_code": "SHIPMENT",
        "minimum_pages": 1,
        "maximum_pages": 8,
    },
    {
        "tenant_id": "maple-accounts",
        "document_code": "INVOICE",
        "minimum_pages": 1,
        "maximum_pages": 5,
    },
]

# A segment lasts five minutes of paced publication. Step Functions carries
# the next offset between invocations, so a 900-second request uses 3 segments.
# The function timeout is 600 seconds, leaving room for slow requests/retries.
SEGMENT_SECONDS = 300


def make_sustained_event(run_id, index, started_at, events_per_second):
    """Create a valid, varied operation whose values survive segment retries."""

    # Rotate through four known customers. The sequence belongs to the whole
    # run, not to a Lambda invocation, so segment boundaries never reuse IDs.
    customer = CUSTOMERS[index % len(CUSTOMERS)]
    sequence = f"{index:09d}"

    # Deterministic bytes provide variation without depending on random state
    # or wall-clock time. About 2% of requests fail; failed requests process no
    # billable pages. Retrying the same run/index produces the same outcome.
    variation = hashlib.sha256(f"{run_id}:{index}".encode()).digest()
    outcome = "FAILED" if variation[0] % 50 == 0 else "SUCCESS"
    page_choices = customer["maximum_pages"] - customer["minimum_pages"] + 1
    page_variation = int.from_bytes(variation[1:5], "big")
    pages = customer["minimum_pages"] + page_variation % page_choices

    if outcome == "FAILED":
        pages = 0

    # Spread business timestamps over the requested duration. Sending occurs
    # in one-second groups, and retries can make arrival later than event_time.
    occurred = started_at + timedelta(seconds=index / events_per_second)

    return {
        "event_id": f"USG-{run_id}-{sequence}",
        "tenant_id": customer["tenant_id"],
        "request_id": (
            f"REQ-{customer['document_code']}-{run_id}-{sequence}"
        ),
        "event_time": occurred.isoformat().replace("+00:00", "Z"),
        "service": "document_extraction",
        "pages_processed": pages,
        "outcome": outcome,
        "contract_version": "v1",
    }


def check_reference_data(started_at, duration_seconds):
    """Fail before publication if a required customer contract/rate is absent."""

    database = boto3.resource("dynamodb")
    contracts = database.Table(os.environ["CONTRACT_TABLE"])
    rates = database.Table(os.environ["RATE_TABLE"])
    finishes_at = started_at + timedelta(seconds=duration_seconds)

    # The seeded sustained customers share one immutable per-page rate. Read
    # it once, then check all four customer contracts using consistent reads.
    rate = rates.get_item(
        Key={"rate_id": "document-v1"},
        ConsistentRead=True,
    ).get("Item")

    if not rate or rate.get("service") != "document_extraction":
        raise ValueError("Seed the document-v1 extraction rate before publishing")

    rate_per_page = Decimal(rate["rate_per_page"])

    if rate.get("currency") != "INR" or rate_per_page != Decimal("0.40"):
        raise ValueError("document-v1 must retain its immutable INR 0.40 rate")

    for customer in CUSTOMERS:
        contract = contracts.get_item(
            Key={
                "tenant_id": customer["tenant_id"],
                "contract_version": "v1",
            },
            ConsistentRead=True,
        ).get("Item")

        if not contract or contract.get("rate_id") != "document-v1":
            raise ValueError(f"Missing v1 contract for {customer['tenant_id']}")

        budget = Decimal(contract["budget_amount"])

        if (
            not budget.is_finite()
            or not 0 < budget <= Decimal("1000000000")
            or budget != budget.quantize(Decimal("0.000001"))
            or not re.fullmatch(
                r"[A-Za-z0-9_-]{1,64}",
                contract.get("budget_policy_version", ""),
            )
        ):
            raise ValueError(f"Invalid budget policy for {customer['tenant_id']}")

        valid_from = datetime.fromisoformat(
            contract["valid_from"].replace("Z", "+00:00")
        )
        valid_to = datetime.fromisoformat(
            contract["valid_to"].replace("Z", "+00:00")
        )

        # valid_to is exclusive. The last event precedes finishes_at, so an
        # interval ending exactly at valid_to is still entirely covered.
        if started_at < valid_from or finishes_at > valid_to:
            raise ValueError(
                f"Requested event times exceed {customer['tenant_id']} contract"
            )


def produce_segment(event, context):
    """Publish one paced segment and return the next offset to Step Functions."""

    # 1. Reject malformed control inputs before creating any stream records.
    #    Limiting rate keeps a one-shard deployment within its write budget,
    #    even when every operation has one deliberate duplicate delivery.
    duration = event.get("duration_seconds", 900)
    rate = event.get("events_per_second", 200)
    duplicate_every = event.get("duplicate_every", 100)
    start_second = event.get("start_second", 0)

    limits = (
        ("duration_seconds", duration, 1, 3600),
        ("events_per_second", rate, 1, 250),
        ("duplicate_every", duplicate_every, 0, 10000),
        ("start_second", start_second, 0, 3599),
    )

    for name, value, minimum, maximum in limits:
        if type(value) is not int or not minimum <= value <= maximum:
            raise ValueError(f"{name} must be an integer in {minimum}..{maximum}")

    if start_second >= duration or start_second % SEGMENT_SECONDS:
        raise ValueError("start_second must be a segment boundary before duration")

    run_id = event["run_id"]
    started_at = datetime.fromisoformat(event["start_time"].replace("Z", "+00:00"))

    if started_at.tzinfo is None:
        raise ValueError("start_time must include a UTC offset")

    started_at = started_at.astimezone(timezone.utc)

    # 2. Check DynamoDB before the first write of each segment. The streaming
    #    job still performs its own API lookup and retains the pricing snapshot.
    check_reference_data(started_at, duration)
    stop_second = min(start_second + SEGMENT_SECONDS, duration)
    stream_name = os.environ["STREAM_NAME"]
    client = boto3.client(
        "kinesis",
        config=Config(
            connect_timeout=5,
            read_timeout=10,
            retries={"mode": "standard", "total_max_attempts": 3},
        ),
    )
    delivered = 0
    pages = 0
    failed = 0

    # 3. Each outer iteration generates one second of traffic. These groups
    #    contain at most 500 records and remain small in memory at any duration.
    for second in range(start_second, stop_second):
        if context.get_remaining_time_in_millis() < 60000:
            raise RuntimeError("Segment nearly expired; retry the same input")

        tick_started = time.monotonic()
        records = []

        for index in range(second * rate, (second + 1) * rate):
            usage = make_sustained_event(run_id, index, started_at, rate)
            business_json = json.dumps(usage, sort_keys=True)
            envelope = json.dumps({"payload": business_json}) + "\n"
            record = {
                "PartitionKey": usage["tenant_id"],
                "Data": envelope.encode(),
            }
            records.append(record)
            pages += usage["pages_processed"]

            if usage["outcome"] == "FAILED":
                failed += 1

            # Duplicates preserve the entire payload, including IDs and time.
            # They add transport records, never additional billable operations.
            if duplicate_every and index % duplicate_every == 0:
                records.append(record)

        publish(client, records, stream_name)
        delivered += len(records)

        # 4. Wait out the remainder of this second. Slow network calls make
        #    the run longer; we never catch up by bursting several seconds of
        #    traffic at once. A successful 900-second workload lasts >=15 min.
        elapsed = time.monotonic() - tick_started
        time.sleep(max(0, 1 - elapsed))

        if (second + 1) % 30 == 0 or second + 1 == stop_second:
            progress = {
                "run_id": run_id,
                "completed_seconds": second + 1,
                "segment_delivery_records": delivered,
                "target_events_per_second": rate,
            }
            print(json.dumps(progress))

    # 5. Advance only after the entire segment has been accepted by Kinesis.
    #    A failed invocation is retried from its original offset. Some records
    #    may be sent again; stable IDs protect warehouse effects, not transport.
    next_input = dict(event)
    next_input["start_second"] = stop_second
    completed_operations = stop_second * rate
    planned_duplicates = 0

    if duplicate_every:
        planned_duplicates = (completed_operations - 1) // duplicate_every + 1

    return {
        "run_id": run_id,
        "complete": stop_second == duration,
        "completed_seconds": stop_second,
        "unique_operations": completed_operations,
        "planned_delivery_records": completed_operations + planned_duplicates,
        "segment": {
            "first_sequence": start_second * rate,
            "unique_operations": (stop_second - start_second) * rate,
            "accepted_delivery_records": delivered,
            "failed_operations": failed,
            "billable_pages": pages,
        },
        "next_input": next_input,
    }


def make_event(run_id, index, start_time, tenant_id, contract_version):
    """Build one completed operation with a repeatable identity and timestamp."""

    # The operation's identity must survive retries. A random UUID here would
    # turn a repeated invocation into new billable operations every time.
    identity = f"{tenant_id}:{run_id}:{index}"
    event_id = hashlib.sha256(identity.encode()).hexdigest()

    # index starts at zero. Successive events have timestamps one second apart,
    # but this changes their business timestamps only; it does not slow sending
    # to one event per second. The supplied start time can be historical.
    occurred = datetime.fromisoformat(start_time.replace("Z", "+00:00"))

    if occurred.tzinfo is None:
        raise ValueError("start_time must include a UTC offset")

    occurred = occurred + timedelta(seconds=index)
    occurred = occurred.astimezone(timezone.utc)

    # Each generated event represents one successful 20-page request. Pricing
    # is deliberately absent: Glue resolves contract_version through the API.
    return {
        "event_id": event_id,
        "tenant_id": tenant_id,
        "request_id": f"{run_id}-{index}",
        "event_time": occurred.isoformat().replace("+00:00", "Z"),
        "service": "document_extraction",
        "pages_processed": 20,
        "outcome": "SUCCESS",
        "contract_version": contract_version,
    }


def publish(client, records, stream_name):
    """Send one group of records, retrying only entries Kinesis rejected."""

    pending = records

    for attempt in range(5):
        response = client.put_records(
            StreamName=stream_name,
            Records=pending,
        )

        # A successful PutRecords request can contain individual failures.
        # Result entries correspond to request entries in the same order.
        # Keep failed records unchanged so a retry preserves their event IDs.
        failed_records = []

        for record, result in zip(pending, response["Records"]):
            if "ErrorCode" in result:
                failed_records.append(record)

        pending = failed_records

        if not pending:
            return

        # Give a temporarily throttled stream time to recover. The delay is
        # capped so one invocation cannot spend its entire lifetime sleeping.
        time.sleep(min(2 ** attempt, 8))

    raise RuntimeError(
        f"Kinesis did not accept {len(pending)} records; rerun the same input"
    )


def lambda_handler(event, context):
    """Read invocation settings, generate events, publish them, and report counts."""

    run_id = event.get("run_id")

    if not isinstance(run_id, str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,40}", run_id):
        raise ValueError("run_id must use 1..40 letters, digits, underscores/hyphens")

    mode = event.get("mode", "count")

    if mode == "sustained":
        return produce_segment(event, context)

    if mode != "count":
        raise ValueError("mode must be count or sustained")

    # 1. Read the control input pasted into the Lambda console. This input is
    #    a generation request, not itself a document-usage event.
    count = int(event.get("count", 100))
    duplicate_every = int(event.get("duplicate_every", 10))
    run_id = event["run_id"]
    start_time = event["start_time"]
    tenant_id = event.get("tenant_id", "customer-017")
    contract_version = event.get("contract_version", "v1")

    # Keep each invocation finite. To retry an interrupted invocation, reuse
    # ALL its values. To produce new operations, choose a different run_id.
    if not 1 <= count <= 10000 or not 0 <= duplicate_every <= 10000:
        raise ValueError(
            "count must be 1..10000; duplicate_every must be 0..10000"
        )

    if not isinstance(run_id, str) or not run_id or len(run_id) > 40:
        raise ValueError("run_id must be a nonempty string of at most 40 characters")

    # 2. Connect using the Lambda execution role. STREAM_NAME identifies the
    #    existing Kinesis stream; this function does not create AWS resources.
    client = boto3.client("kinesis")
    stream_name = os.environ["STREAM_NAME"]
    records = []
    delivered = 0

    for index in range(count):
        usage = make_event(
            run_id=run_id,
            index=index,
            start_time=start_time,
            tenant_id=tenant_id,
            contract_version=contract_version,
        )

        # 3. Wrap the business JSON in a string-valued payload field. Glue can
        #    then quarantine invalid business JSON without losing the original
        #    text. The trailing newline makes Firehose output readable as JSONL.
        business_json = json.dumps(usage, sort_keys=True)
        envelope = json.dumps({"payload": business_json}) + "\n"

        record = {
            "PartitionKey": usage["tenant_id"],
            "Data": envelope.encode(),
        }
        records.append(record)

        # 4. Insert deliberate duplicate deliveries to exercise deduplication.
        #    For count=100 and duplicate_every=10, indexes 0,10,...,90 are
        #    repeated: 100 distinct operations produce 110 delivery records.
        #    A value of zero disables these deliberate duplicates.
        if duplicate_every and index % duplicate_every == 0:
            records.append(record)

        # 5. Flush small groups, including the final incomplete group. Sending
        #    groups is more efficient than one Kinesis request per operation.
        if len(records) >= 100 or index == count - 1:
            if context.get_remaining_time_in_millis() < 40000:
                raise RuntimeError(
                    "Invocation nearly expired; rerun the identical input"
                )

            publish(client, records, stream_name)
            delivered += len(records)
            records = []

            # This short pause separates publication groups. It is not a
            # configurable real-time event rate or a continuous scheduler.
            time.sleep(0.2)

    # These counts confirm Kinesis acceptance only. Glue and Redshift process
    # the records asynchronously, so query the warehouse separately afterward.
    return {
        "unique_operations": count,
        "delivered_records": delivered,
        "expected_pages": count * 20,
        "run_id": run_id,
    }
