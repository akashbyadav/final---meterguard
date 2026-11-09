"""Process live usage or replay archived usage with the same pricing rules.

Live path: Kinesis -> validation -> lookup API -> S3 staging -> Redshift commit.
Replay path: a bounded S3 raw selection -> the same validation and commit path.
Job arguments are documented in config/glue-parameters.json and runbook.md.
"""

import hashlib
import json
import re
import time
import uuid
from datetime import datetime, timezone
from decimal import Decimal
from itertools import islice

# This order matches mg.usage_fact in sql/schema.sql. Quarantine rows use the
# same intermediate Spark schema but leave these fact fields empty.
FACT_COLUMNS = [
    "tenant_id",
    "event_id",
    "processing_version",
    "request_id",
    "event_time",
    "billing_period",
    "service",
    "pages_processed",
    "outcome",
    "contract_version",
    "rate_id",
    "currency",
    "rate_per_page",
    "estimated_charge",
    "budget_amount",
    "budget_policy_version",
    "source_hash",
    "result_hash",
]

# Increment this when processing behaviour changes. Formatting and comments
# alone do not require a new version or invalidate an existing checkpoint.
CODE_VERSION = "1"


def digest(value):
    """Hash a stable JSON representation for duplicate/conflict detection."""

    encoded = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode()

    return hashlib.sha256(encoded).hexdigest()


def utc(value):
    """Parse an explicit timezone and normalize its instant to UTC."""

    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))

    if parsed.tzinfo is None:
        raise ValueError("Timestamp must include a timezone")

    return parsed.astimezone(timezone.utc)


def validate_payload(payload):
    """Validate one business event and return its normalized dictionary."""

    # Validate the inner payload string, not the Kinesis transport envelope.
    # A bounded size prevents a single malformed event from consuming excess
    # executor memory before we even parse its business fields.
    if not isinstance(payload, str) or len(payload.encode()) > 65536:
        raise ValueError("Payload must be a JSON string no larger than 64 KiB")

    usage = json.loads(payload)
    required_fields = {
        "tenant_id",
        "event_id",
        "request_id",
        "event_time",
        "service",
        "pages_processed",
        "outcome",
        "contract_version",
    }

    if not isinstance(usage, dict) or set(usage) != required_fields:
        raise ValueError("Payload fields do not match the usage contract")

    # Match identifier lengths to their warehouse columns. These checks also
    # keep lookup keys and downstream identity strings unambiguous.
    for field in ("tenant_id", "contract_version", "service"):
        value = usage[field]

        if (
            not isinstance(value, str)
            or not re.fullmatch(r"[A-Za-z0-9_-]{1,64}", value)
        ):
            raise ValueError(f"Invalid {field}")

    for field in ("event_id", "request_id"):
        value = usage[field]

        if (
            not isinstance(value, str)
            or not re.fullmatch(r"[A-Za-z0-9_-]{1,128}", value)
        ):
            raise ValueError(f"Invalid {field}")

    # bool is a subclass of int in Python. An exact type check prevents True
    # from accidentally becoming one billable page.
    pages = usage["pages_processed"]

    if type(pages) is not int or not 0 <= pages <= 1000000:
        raise ValueError("pages_processed must be an integer between 0 and 1000000")

    if usage["outcome"] not in ("SUCCESS", "FAILED"):
        raise ValueError("outcome must be SUCCESS or FAILED")

    occurred = utc(usage["event_time"])

    if not 2020 <= occurred.year <= 2100:
        raise ValueError("event_time is outside the supported range")

    # Equivalent timestamp offsets must produce the same source hash.
    # Otherwise identical events could be rejected as conflicting duplicates.
    usage["event_time"] = occurred.isoformat(timespec="microseconds")

    return usage


def make_fact(usage, reference, processing_version):
    """Price a validated event using its immutable historical reference."""

    occurred = utc(usage["event_time"])
    valid_from = utc(reference["valid_from"])
    valid_to = utc(reference["valid_to"])

    # A contract interval includes its start and excludes its end. At the rate
    # change boundary, only the new contract version is valid.
    if not valid_from <= occurred < valid_to:
        raise ValueError("CONTRACT_OUTSIDE_EFFECTIVE_PERIOD")

    if usage["service"] != reference["service"]:
        raise ValueError("SERVICE_NOT_IN_CONTRACT")

    # Decimal arithmetic preserves exact charge estimates. Invalid reference
    # amounts fail processing rather than misclassify usage as a bad event.
    rate = Decimal(reference["rate_per_page"])
    budget = Decimal(reference["budget_amount"])
    amount_limits = (
        (rate, Decimal("1000")),
        (budget, Decimal("1000000000")),
    )

    for value, maximum in amount_limits:
        if not value.is_finite() or value <= 0 or value > maximum:
            raise RuntimeError("Reference amounts are invalid")

        if value != value.quantize(Decimal("0.000001")):
            raise RuntimeError("Reference amounts support at most six decimal places")

    if not re.fullmatch(r"[A-Z]{3}", reference["currency"]):
        raise RuntimeError("Reference currency must have three uppercase letters")

    for field in ("rate_id", "budget_policy_version"):
        if not re.fullmatch(r"[A-Za-z0-9_-]{1,64}", reference[field]):
            raise RuntimeError(f"Invalid reference {field}")

    # Only successful operations contribute a charge. A failed operation still
    # becomes a fact so reports can include its outcome without billing it.
    estimated_charge = Decimal(0)

    if usage["outcome"] == "SUCCESS":
        estimated_charge = rate * usage["pages_processed"]

    fact = dict(usage)
    fact.update({
        "processing_version": processing_version,
        "event_time": occurred.strftime("%Y-%m-%d %H:%M:%S.%f"),
        "billing_period": occurred.strftime("%Y-%m-01"),
        "rate_id": reference["rate_id"],
        "currency": reference["currency"],
        "rate_per_page": rate.quantize(Decimal("0.000001")),
        "estimated_charge": estimated_charge.quantize(Decimal("0.000001")),
        "budget_amount": budget.quantize(Decimal("0.000001")),
        "budget_policy_version": reference["budget_policy_version"],
        "source_hash": digest(usage),
    })

    # source_hash identifies the normalized input; result_hash additionally
    # covers the resolved pricing and processing version. Redshift rejects an
    # existing business key if its result changes.
    fact["result_hash"] = digest(fact)

    return fact


def describe_context_error(error):
    """Explain an API rejection without logging signed headers or raw bodies."""

    # API Gateway uses HTTP 403 for several different configuration problems.
    # Preserve its error type and request ID so an operator can locate the
    # rejected request in API logs instead of seeing only "Forbidden" in Spark.
    headers = error.headers or {}
    error_type = headers.get("x-amzn-errortype", "unknown")
    request_id = headers.get("x-amzn-requestid", "unknown")

    # Only print these bounded metadata fields. Signature-error bodies can
    # echo parts of a signed request, so never print the complete response.
    error_type = re.sub(r"[^A-Za-z0-9_.:-]", "", error_type)[:128]
    request_id = re.sub(r"[^A-Za-z0-9_-]", "", request_id)[:128]
    message = ""

    try:
        payload = json.loads(error.read(8192).decode("utf-8", errors="replace"))

        if isinstance(payload, dict):
            message = str(payload.get("message", "")).lower()

    except (OSError, ValueError):
        # HTML, an empty body, or a truncated error must not hide the original
        # HTTP failure. Its request ID/type still support investigation.
        pass

    finally:
        error.close()

    reason = "Inspect API Gateway logs using the request_id"
    kind = error_type.lower()

    if "missing authentication token" in message or "missingauthentication" in kind:
        reason = (
            "Missing authentication token: check the deployed stage, exact POST "
            "resource path, and IAM signing"
        )
    elif "not authorized" in message or "accessdenied" in kind:
        reason = (
            "Access denied: check the actual Glue role's execute-api:Invoke "
            "ARN, API resource policy, and any explicit deny"
        )
    elif "expired" in message or "expired" in kind:
        reason = "Expired signature/token: check temporary credentials and clock"
    elif "signature" in message or "signature" in kind or "scoped" in message:
        reason = (
            "Signature rejected: check REGION against the endpoint Region, "
            "execute-api signing, and temporary credentials"
        )
    elif "api key" in message or "apikey" in kind:
        reason = "API key required or invalid: this project's method requires no key"
    elif error.code == 403:
        reason = (
            "Forbidden: check API key requirement, endpoint settings, resource "
            "policy, and WAF; inspect API logs for the exact rejection"
        )

    return (
        f"Context API failed with HTTP {error.code}; "
        f"error_type={error_type}; request_id={request_id}; {reason}"
    )


def lookup_context(keys, url, region):
    """Call the IAM-authorized lookup API with bounded transient retries."""

    # Keep AWS imports in execution functions so pure validation/pricing can
    # be checked locally without loading the Glue or Spark runtime.
    import boto3
    from botocore.auth import SigV4Auth
    from botocore.awsrequest import AWSRequest
    from urllib.error import HTTPError, URLError
    from urllib.request import Request, urlopen

    body = json.dumps({"keys": keys}).encode()
    session = boto3.Session(region_name=region)

    for attempt in range(4):
        # Sign every attempt with current temporary credentials. Reusing a
        # stale signature after credential rotation could fail an otherwise
        # valid request. The API method uses AWS_IAM authorization.
        credentials = session.get_credentials().get_frozen_credentials()
        signed_request = AWSRequest(
            method="POST",
            url=url,
            data=body,
            headers={"Content-Type": "application/json"},
        )
        SigV4Auth(credentials, "execute-api", region).add_auth(signed_request)

        http_request = Request(
            url,
            data=body,
            headers=dict(signed_request.headers),
            method="POST",
        )

        try:
            with urlopen(http_request, timeout=8) as reply:
                return json.loads(reply.read())["contexts"]

        except HTTPError as error:
            # Throttling and temporary service failures are retryable. A bad
            # URL or authorization error must surface instead of retry forever.
            retryable_statuses = (429, 500, 502, 503, 504)

            if error.code not in retryable_statuses or attempt == 3:
                raise RuntimeError(describe_context_error(error)) from error

            # Release the response before retrying a temporary HTTP failure.
            error.close()

        except (URLError, TimeoutError):
            if attempt == 3:
                raise

        time.sleep(0.5 * 2 ** attempt)

    raise RuntimeError("Context lookup exhausted retries")


def enrich_partition(rows, url, region, processing_version):
    """Validate and enrich bounded groups of rows on a Spark executor."""

    # Work in groups of 50 inside each partition. This avoids collecting the
    # stream on the driver or making a separate HTTP call for every event.
    iterator = iter(rows)

    while True:
        group = list(islice(iterator, 50))

        if not group:
            return

        valid = []

        for row in group:
            payload = row["payload"]

            try:
                usage = validate_payload(payload)
                valid.append((payload, usage))

            except (ValueError, TypeError, AttributeError) as error:
                # Valid and rejected rows share one intermediate schema.
                # Keep the original text and reason for durable quarantine.
                yield {
                    **dict.fromkeys(FACT_COLUMNS),
                    "record_type": "REJECTED",
                    "raw_payload": payload,
                    "reason": str(error)[:256],
                }

        if not valid:
            continue

        # Multiple events can share one customer/contract pair. Resolve that
        # pair once per group, then attach the returned context to each event.
        keys = sorted({
            (usage["tenant_id"], usage["contract_version"])
            for _, usage in valid
        })
        lookup_keys = [
            {"tenant_id": tenant, "contract_version": version}
            for tenant, version in keys
        ]
        contexts = lookup_context(lookup_keys, url, region)
        references = {
            (item["tenant_id"], item["contract_version"]): item
            for item in contexts
        }

        # An incomplete API response is a processing failure. It must not
        # silently discard a valid operation from the current micro-batch.
        if set(references) != set(keys):
            raise RuntimeError("Context API returned an incomplete response")

        for payload, usage in valid:
            reference_key = (usage["tenant_id"], usage["contract_version"])
            reference = references[reference_key]

            try:
                if "error" in reference:
                    raise ValueError(reference["error"])

                fact = make_fact(usage, reference, processing_version)

                yield {
                    **fact,
                    "record_type": "VALID",
                    "raw_payload": None,
                    "reason": None,
                }

            except ValueError as error:
                # Missing/invalid business context is a durable rejection.
                # RuntimeError and service errors propagate so Spark fails the
                # batch and leaves its checkpoint available for recovery.
                yield {
                    **dict.fromkeys(FACT_COLUMNS),
                    "record_type": "REJECTED",
                    "raw_payload": payload,
                    "reason": str(error)[:256],
                }


def run():
    """Configure the job, protect its identity, then run live or bounded input."""

    import sys

    import boto3
    from botocore.exceptions import ClientError
    from awsglue.context import GlueContext
    from awsglue.utils import getResolvedOptions
    from pyspark.context import SparkContext
    from pyspark.sql import functions as F
    from pyspark.sql.types import (
        DecimalType,
        IntegerType,
        StringType,
        StructField,
        StructType,
    )

    # 1. Read explicit job arguments. Replay arguments remain present in live
    #    mode with value "unused", keeping one configuration shape for both jobs.
    names = [
        "JOB_NAME",
        "REGION",
        "BUCKET",
        "STREAM_ARN",
        "CONTEXT_URL",
        "WORKGROUP_NAME",
        "DATABASE",
        "COPY_ROLE_ARN",
        "GENERATION",
        "PROCESSING_VERSION",
        "RUN_MODE",
        "REPLAY_PREFIX",
        "REPLAY_FROM",
        "REPLAY_TO",
        "FAIL_AFTER_COMMIT_BATCH",
    ]
    args = getResolvedOptions(sys.argv, names)

    if args["RUN_MODE"] not in ("live", "replay"):
        raise ValueError("RUN_MODE must be live or replay")

    for name in ("GENERATION", "PROCESSING_VERSION"):
        if not re.fullmatch(r"[A-Za-z0-9_-]{1,64}", args[name]):
            raise ValueError(f"Invalid {name}")

    if not args["CONTEXT_URL"].startswith("https://"):
        raise ValueError("CONTEXT_URL must use HTTPS")

    if args["RUN_MODE"] == "replay":
        # Stop the live job before replaying. The allowed archive location and
        # half-open event-time range make this a bounded recovery operation.
        archive_root = f"s3://{args['BUCKET']}/raw/"

        if not args["REPLAY_PREFIX"].startswith(archive_root):
            raise ValueError("REPLAY_PREFIX must be inside this bucket's raw/ archive")

        if utc(args["REPLAY_FROM"]) >= utc(args["REPLAY_TO"]):
            raise ValueError("REPLAY_FROM must precede REPLAY_TO")

    # 2. Create the managed Spark context and AWS clients. Four enrichment
    #    partitions bound concurrent lookup traffic; increasing this requires
    #    checking API, Lambda, and DynamoDB capacity together.
    glue = GlueContext(SparkContext.getOrCreate())
    spark = glue.spark_session
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.conf.set("spark.sql.shuffle.partitions", "4")

    s3 = boto3.client("s3", region_name=args["REGION"])
    data = boto3.client("redshift-data", region_name=args["REGION"])
    bucket = args["BUCKET"]
    generation = args["GENERATION"]
    root = f"s3://{bucket}/"

    # These local functions share this job's arguments and clients. Only
    # enrich_partition runs on executors; S3 coordination and commits stay here.
    def read_json(key):
        """Read a coordination object, returning None only when it is absent."""

        try:
            response = s3.get_object(Bucket=bucket, Key=key)
            return json.loads(response["Body"].read())

        except ClientError as error:
            if error.response["Error"]["Code"] == "NoSuchKey":
                return None

            # Permission and connectivity errors are not missing objects.
            raise

    def create_json(key, value):
        """Publish a JSON object only if the key does not already exist."""

        # Conditional creation protects completed pointers from replacement.
        # Partial attempts use separate directories and never become ready.
        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=json.dumps(value).encode(),
            ContentType="application/json",
            IfNoneMatch="*",
        )

    # 3. Bind one generation to one query and checkpoint. JobRunId changes on
    #    every restart and must never be used as the durable generation value.
    identity_names = (
        "RUN_MODE",
        "REGION",
        "STREAM_ARN",
        "CONTEXT_URL",
        "WORKGROUP_NAME",
        "DATABASE",
        "COPY_ROLE_ARN",
        "PROCESSING_VERSION",
        "REPLAY_PREFIX",
        "REPLAY_FROM",
        "REPLAY_TO",
    )
    identity = {name: args[name] for name in identity_names}
    identity["code_version"] = CODE_VERSION
    identity_key = f"checkpoints/{generation}/identity.json"
    previous = read_json(identity_key)

    if previous is None:
        try:
            create_json(identity_key, identity)

        except ClientError as error:
            # If creation raced another attempt, read the winning value below
            # and require it to match rather than overwrite it.
            expected_conflicts = (
                "PreconditionFailed",
                "ConditionalRequestConflict",
            )

            if error.response["Error"]["Code"] not in expected_conflicts:
                raise

        previous = read_json(identity_key)

    if previous != identity:
        raise ValueError(
            "Generation configuration changed; use a new generation for a new query"
        )

    if args["RUN_MODE"] == "live":
        # A deleted checkpoint could recycle batch IDs and incorrectly reuse
        # old ledger entries. A used generation must retain its Spark metadata.
        prior_files = s3.list_objects_v2(
            Bucket=bucket,
            Prefix=f"staged/{generation}/",
            MaxKeys=1,
        )

        if prior_files.get("KeyCount", 0):
            checkpoint_metadata = read_json(
                f"checkpoints/{generation}/spark/metadata"
            )

            if checkpoint_metadata is None:
                raise ValueError(
                    "Used generation has lost its Spark checkpoint; "
                    "recover with a new generation"
                )

    # 4. Define the shared intermediate row schema explicitly. Amounts use
    #    fixed precision; rejected rows can leave fact fields null.
    decimal_columns = {"rate_per_page", "estimated_charge", "budget_amount"}
    fields = []

    for name in FACT_COLUMNS + ["record_type", "raw_payload", "reason"]:
        if name in decimal_columns:
            field_type = DecimalType(18, 6)
        elif name == "pages_processed":
            field_type = IntegerType()
        else:
            field_type = StringType()

        fields.append(StructField(name, field_type, True))

    output_schema = StructType(fields)

    def commit_ready(ready, batch_id):
        """Commit a completed manifest and wait for Redshift's final outcome."""

        # Pass business values separately from SQL. The procedure validates
        # batch identity and commits facts, alert intent, and its ledger together.
        values = {
            "generation": generation,
            "batch_id": str(batch_id),
            "version": args["PROCESSING_VERSION"],
            "manifest": ready["manifest_uri"],
            "role": args["COPY_ROLE_ARN"],
            "input_count": str(ready["input_count"]),
            "valid_count": str(ready["valid_count"]),
            "rejected_count": str(ready["rejected_count"]),
            "publish": "true" if args["RUN_MODE"] == "live" else "false",
        }
        parameters = [
            {"name": name, "value": value}
            for name, value in values.items()
        ]
        commit_sql = """
            CALL mg.commit_batch(
                :generation,
                CAST(:batch_id AS BIGINT),
                :version,
                :manifest,
                :role,
                CAST(:input_count AS BIGINT),
                CAST(:valid_count AS BIGINT),
                CAST(:rejected_count AS BIGINT),
                CAST(:publish AS BOOLEAN)
            )
        """
        statement_id = data.execute_statement(
            WorkgroupName=args["WORKGROUP_NAME"],
            Database=args["DATABASE"],
            Sql=commit_sql,
            Parameters=parameters,
        )["Id"]

        # Submission alone is insufficient. Returning from the batch callback
        # permits Spark to finish its checkpoint, so wait for a committed result.
        deadline = time.monotonic() + 600

        while time.monotonic() < deadline:
            result = data.describe_statement(Id=statement_id)

            if result["Status"] == "FINISHED":
                return

            if result["Status"] in ("FAILED", "ABORTED"):
                raise RuntimeError(result.get("Error", result["Status"]))

            time.sleep(2)

        # A timeout can race a successful commit. Fail rather than acknowledge
        # the batch; the same generation/manifest safely resolves that on retry.
        data.cancel_statement(Id=statement_id)
        raise TimeoutError(
            "Warehouse outcome uncertain; restart with the same generation"
        )

    def process_batch(batch, batch_id):
        """Enrich, freeze output, and commit one live or replay batch."""

        if batch.isEmpty():
            return

        # A completed pointer means enrichment already finished. Reuse the
        # exact files on retry, including their original pricing snapshot.
        ready_key = f"staged/{generation}/{batch_id}/ready.json"
        ready = read_json(ready_key)

        if ready is None:
            # 5. Run HTTP lookups on executors. Persist the result before counts
            #    and writes so normal repeated Spark actions reuse enrichment.
            payload_rows = batch.select("payload").repartition(4).rdd
            enriched_rows = payload_rows.mapPartitions(
                lambda rows: enrich_partition(
                    rows,
                    args["CONTEXT_URL"],
                    args["REGION"],
                    args["PROCESSING_VERSION"],
                )
            )
            prepared = spark.createDataFrame(
                enriched_rows,
                output_schema,
            ).persist()

            try:
                # Only these two aggregate counts reach the driver. Individual
                # usage records remain distributed across Spark partitions.
                count_rows = prepared.groupBy("record_type").count().collect()
                counts = {
                    row["record_type"]: row["count"]
                    for row in count_rows
                }
                valid_count = counts.get("VALID", 0)
                rejected_count = counts.get("REJECTED", 0)

                # Every attempt writes to a new directory. An interrupted write
                # cannot overwrite files referenced by an earlier ready marker.
                attempt = f"staged/{generation}/{batch_id}/{uuid.uuid4().hex}/"
                entries = []

                if valid_count:
                    facts_prefix = attempt + "facts/"
                    valid_facts = prepared.filter(
                        "record_type = 'VALID'"
                    ).select(*FACT_COLUMNS)
                    valid_facts.write.json(root + facts_prefix)

                    # 6. Build COPY's manifest from completed JSON part files.
                    #    Exclude Spark metadata such as _SUCCESS.
                    paginator = s3.get_paginator("list_objects_v2")

                    for page in paginator.paginate(
                        Bucket=bucket,
                        Prefix=facts_prefix,
                    ):
                        for item in page.get("Contents", []):
                            if item["Key"].endswith(".json"):
                                entries.append({
                                    "url": root + item["Key"],
                                    "mandatory": True,
                                })

                    if not entries:
                        raise RuntimeError("Spark produced no fact files")

                if rejected_count:
                    # Quarantine must also finish before publishing ready.json.
                    # Its original payload and reason support investigation.
                    rejected_rows = prepared.filter(
                        "record_type = 'REJECTED'"
                    ).select("raw_payload", "reason")
                    quarantine_path = (
                        root
                        + f"quarantine/{generation}/{batch_id}/{uuid.uuid4().hex}/"
                    )
                    rejected_rows.write.json(quarantine_path)

                # 7. Publish the manifest first, then its ready pointer. A later
                #    retry sees either no pointer or a fully completed output.
                manifest_key = attempt + "manifest.json"
                create_json(manifest_key, {"entries": entries})

                ready = {
                    "manifest_uri": root + manifest_key,
                    "input_count": valid_count + rejected_count,
                    "valid_count": valid_count,
                    "rejected_count": rejected_count,
                }
                create_json(ready_key, ready)

            finally:
                # Release cached data after either success or failure.
                prepared.unpersist()

        # 8. Commit before returning control to Spark. The stored procedure's
        #    ledger makes the commit safe to repeat after an interrupted callback.
        commit_ready(ready, batch_id)

        print(json.dumps({
            "generation": generation,
            "batch_id": batch_id,
            **ready,
            "status": "COMMITTED",
        }))

        if args["FAIL_AFTER_COMMIT_BATCH"] in ("any", str(batch_id)):
            # Deliberately exercise the commit-before-checkpoint crash window.
            # Disable this switch before restarting; retain the same generation.
            raise RuntimeError(
                "Injected failure AFTER Redshift commit "
                "and BEFORE checkpoint completion"
            )

    # 9. Choose the input path. Both modes call process_batch; only live mode
    #    publishes new alert intent through commit_ready's publish parameter.
    if args["RUN_MODE"] == "live":
        stream = glue.create_data_frame.from_options(
            connection_type="kinesis",
            connection_options={
                "streamARN": args["STREAM_ARN"],
                "startingPosition": "TRIM_HORIZON",
                "classification": "json",
                "inferSchema": "false",
                "schema": "payload STRING",
                "failOnDataLoss": "true",
                "emitConsumerLagMetrics": "true",
                "maxFetchRecordsPerShard": "10000",
                "maxRecordPerRead": "1000",
            },
        )

        # An existing checkpoint takes precedence over the initial position.
        # Bounded API retries happen above; an exhausted batch failure surfaces
        # to the job so it can be recovered with this same checkpoint.
        glue.forEachBatch(
            frame=stream,
            batch_function=process_batch,
            options={
                "windowSize": "30 seconds",
                "checkpointLocation": root + f"checkpoints/{generation}/spark/",
                "batchMaxRetries": 0,
            },
        )

    else:
        # FAILFAST checks the outer JSON envelope. The inner payload remains
        # text so malformed business records can still be quarantined.
        archive = (
            spark.read
            .schema("payload STRING")
            .option("mode", "FAILFAST")
            .option("recursiveFileLookup", "true")
            .json(args["REPLAY_PREFIX"])
        )
        event_time = F.to_timestamp(
            F.get_json_object("payload", "$.event_time")
        )

        # The S3 prefix is based on arrival time. Filter separately by business
        # event time, including invalid timestamps so validation can reject them.
        start = F.lit(utc(args["REPLAY_FROM"]))
        end = F.lit(utc(args["REPLAY_TO"]))
        within_range = (event_time >= start) & (event_time < end)
        selected = archive.filter(event_time.isNull() | within_range)

        # Bounded replay has a single batch ID. Its independent generation
        # prevents that ID from colliding with any live batch ledger entry.
        process_batch(selected, 0)


if __name__ == "__main__":
    run()
