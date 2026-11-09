# MeterGuard setup and operating runbook

Follow the sections in order. All resources are created through the AWS Management Console. The Python producer also runs on AWS. Configuration files are copy sources for the corresponding console editors; they do not provision resources.

## 1. Prepare the account and record identifiers

Use one AWS account and one Region throughout. `us-east-1` is a suitable starting Region. The console identity needs permission to create the resources below, create/pass their IAM roles, edit resource policies, and administer the Redshift database. Organization policies and permission boundaries can restrict those actions even when an identity has an administrator policy. Resolve account access before creating the pipeline.

Open IAM, S3, DynamoDB, Kinesis, Firehose, Lambda, API Gateway, Redshift, SQS, SNS, Step Functions, EventBridge, and CloudWatch in separate console tabs as needed. Console labels can change; match the named resource and setting if its position differs.

Record these values locally. Substitute them in configuration templates; do not store account credentials in the repository.

| Placeholder | Value to record |
|---|---|
| `REGION` | Selected AWS Region, for example `us-east-1`. |
| `ACCOUNT_ID` | The 12-digit account ID from the account menu. |
| `BUCKET_NAME` | A globally unique name, for example `meterguard-123456789012-us-east-1`. |
| `API_ID` | REST API ID created in section 6. |
| `WORKGROUP_ID` | Redshift Serverless workgroup ID created in section 5. This is different from its name. |
| `TOPIC_ARN` | The SNS topic ARN created in section 10. |
| `PRODUCER_VERSION` | Numeric published `mg-producer` version created in section 8. |

Resource names beginning with `mg-` below are deliberately fixed. If renamed, update IAM resources, SQL IAM user names, Lambda environment variables, and the workflow definition consistently.

The selected configuration uses service-managed encryption: S3 SSE-S3, DynamoDB default encryption, Kinesis `alias/aws/kinesis`, SQS SSE-SQS, and Redshift default encryption. A customer-managed KMS key requires additional key policies and execution-role grants. The supplied policies assume the service-managed choices.

## 2. Create the S3 bucket and upload the Glue script

1. In **S3 → Buckets → Create bucket**, select a general-purpose bucket in `REGION` and enter `BUCKET_NAME`.
2. Keep **Object Ownership: ACLs disabled** and all **Block Public Access** settings enabled.
3. Enable **Bucket Versioning**. Keep default encryption **SSE-S3**. Create the bucket.
4. Open the bucket and create these folders: `scripts`, `raw`, `staged`, `quarantine`, `checkpoints`, `spark-logs`, and `firehose-errors`.
5. Open `scripts/`, choose **Upload**, and upload `glue/usage_stream.py`. Confirm the object is `scripts/usage_stream.py`.
6. Under **Permissions → Bucket policy**, add the policy below after replacing the bucket name. It requires TLS without blocking normal AWS SDK requests.

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "RequireTlsForProjectStorage",
            "Effect": "Deny",
            "Principal": "*",
            "Action": "s3:*",
            "Resource": [
                "arn:aws:s3:::BUCKET_NAME",
                "arn:aws:s3:::BUCKET_NAME/*"
            ],
            "Condition": {
                "Bool": {
                    "aws:SecureTransport": "false"
                }
            }
        }
    ]
}
```

Keep `raw/`, completed `staged/` files/manifests, `checkpoints/`, and the warehouse ledger for the entire supported recovery period. Do not apply an automatic expiration rule to active checkpoints or referenced manifests. Versioning is recovery assistance, not an immutable retention lock. Restrict administrative deletion separately if the operating environment requires it.

## 3. Create and populate the DynamoDB tables

For each table, open **DynamoDB → Tables → Create table**. Use the keys below, on-demand capacity, default encryption, and no secondary indexes or streams.

| Table | Partition key, String | Sort key, String |
|---|---|---|
| `mg-contracts` | `tenant_id` | `contract_version` |
| `mg-rates` | `rate_id` | None |
| `mg-cases` | `case_id` | None |

1. Wait until all three tables are **Active**.
2. Open each table's **Backups** settings and enable point-in-time recovery if required for the account's retention policy. Keep reference and case retention at least as long as the replay horizon; do not enable TTL on these items.
3. Open `config/reference-items.json`. It contains eight individual items: two rates and six contracts. Create every item, including the four named customers used by sustained generation.
4. For `mg-rates`, choose **Explore table items → Create item → JSON view**. Select **DynamoDB JSON**, paste the first item from the `mg-rates` array, and create it. Repeat for the second item.
5. Repeat for all six items in the `mg-contracts` array. Paste one item object at a time, not the enclosing file or its `_instructions` field.
6. Leave `mg-cases` empty; the workflow creates its records.

The fixed dates provide reproducible inputs. For `customer-017`, contract `v1` applies before `2026-10-18T12:00:00Z`; `v2` applies from that instant. Both use the same monthly INR 500 budget policy, while their rates are INR 0.40 and INR 0.30 per page. The four named customers have separate `v1` contracts valid throughout 2026–2099 at INR 0.40 per page, with their own monthly budgets. Dates are event times, not a requirement to execute on that calendar date.

Published contracts and rates are immutable. Create a new contract version and rate ID for changes. Never edit an existing rate in place to test a change; the pipeline deliberately rejects a different result for an existing event key. Budget/currency changes within the same customer's billing month are rejected by the warehouse procedure. New budget policies should begin in a new billing period for this implementation.

## 4. Create the Kinesis stream

1. Open **Kinesis → Data streams → Create data stream**.
2. Name it `mg-usage-stream`; choose **Provisioned** with **1 shard** for the supplied workload.
3. After it becomes **Active**, open **Configuration** and set retention to **72 hours**.
4. Enable server-side encryption using the AWS-managed key `alias/aws/kinesis`.
5. Copy the stream ARN. The producer partitions by customer. This groups a customer's events onto a shard, but the implementation does not assume that producer retries preserve event-time order.

Increase shard capacity only after observing throughput and lag. Glue and Firehose both consume the stream. Retention is a recovery boundary, not an archive substitute.

## 5. Create the Redshift role, namespace, and workgroup

### Create the COPY role

1. Open `config/iam-policies.json`, replace `REGION`, `ACCOUNT_ID`, and `BUCKET_NAME` in the `mg-redshift-copy-role` entry.
2. In **IAM → Roles → Create role**, choose **Custom trust policy** and paste that entry's `trust_policy` object.
3. Continue without attaching a broad managed policy; name the role **exactly** `mg-redshift-copy-role`.
4. Open the role → **Permissions → Add permissions → Create inline policy → JSON**. Paste the entry's `permissions_policy` object, review it, and name it `mg-redshift-copy-access`.

### Create Serverless resources

1. Open **Amazon Redshift → Serverless → Workgroup configuration → Create workgroup**. Choose the explicit configuration flow rather than accepting an unrelated default workgroup.
2. Name the workgroup `mg-workgroup` and create a namespace named `mg-namespace`. Use database `dev`.
3. Configure administrator access through the console. Use a strong administrator password or the console's managed credential option; application roles will use IAM credentials instead.
4. For namespace permissions, associate `mg-redshift-copy-role` and make it the default associated role.
5. Choose a base capacity of **8 RPUs** where available, or the smallest value the Region supports. Configure a maximum capacity and account budget appropriate to the workload.
6. Select the default VPC and the required subnets across Availability Zones. If the account has no default VPC, create a dedicated VPC through **VPC → Create VPC → VPC and more**, with three Availability Zones and one subnet in each; no NAT gateway is needed for the described configuration.
7. Use a security group without public inbound database access. Keep **Publicly accessible: Off** and **Enhanced VPC routing: Off** for this path. Query Editor and the Data API are used instead of direct JDBC connections.
8. Create the workgroup and wait until it is **Available**. Record its **workgroup ID** and ARN, not just the name.

If enhanced VPC routing is required by your account, create an S3 gateway endpoint in that VPC, associate all selected subnet route tables, and allow this bucket in its endpoint policy before using `COPY`. Lambda and Glue in this runbook use public AWS service endpoints without VPC attachment; do not attach them to private subnets without supplying the necessary service endpoints or egress.

## 6. Create the API container

1. Open **API Gateway → Create API → REST API → Build**. Select the standard REST API, not a private REST API or WebSocket API.
2. Create `mg-api` with **Regional** endpoint type.
3. Record its `API_ID`. The resources and methods are created after the Lambda functions exist.

## 7. Create the remaining execution roles

Replace all placeholders in `config/iam-policies.json`, now including `API_ID` and `WORKGROUP_ID`. It is a collection of named policies, not one policy that can be pasted into IAM.

For each role below:

1. **IAM → Roles → Create role → Custom trust policy**: paste that role's `trust_policy`.
2. Create the role with the exact name in the table.
3. Open it and add an inline policy using only its `permissions_policy` object. Give the inline policy a descriptive name such as `mg-glue-access`.
4. Review the service/resource names and resolve any IAM editor errors. Do not paste the outer `roles` object or `_instructions` into a policy editor.

| Role | Trusted service | Purpose |
|---|---|---|
| `mg-producer-role` | Lambda | Read required reference items before generation, write usage records and function logs. |
| `mg-context-role` | Lambda | Read contracts/rates and write logs. |
| `mg-cases-role` | Lambda | Conditional case writes, status reads, and logs. |
| `mg-starter-role` | Lambda | Consume the alert queue and start/inspect its workflow. |
| `mg-dispatcher-role` | Lambda | Access the Redshift outbox, send SQS messages, and log. |
| `mg-glue-role` | Glue | Read Kinesis/archive, call the context API, stage data, checkpoint, and call the commit procedure. |
| `mg-firehose-role` | Firehose | Independently archive Kinesis records to S3. |
| `mg-workflow-role` | Step Functions | Invoke case actions, publish SNS messages, and configure execution logging. |
| `mg-producer-workflow-role` | Step Functions | Invoke published versions of the producer and configure execution logging. |

The policy ARNs can refer to the queue, topic, and state machine before those resources exist. The operator needs `iam:PassRole` for these specific roles when selecting them in service consoles. The `operator-api-invoke-policy` entry is an optional permission fragment for an operator who will call the deployed acknowledgement API; it is not a resource-provisioning policy.

Data API statement actions use `Resource: "*"` with a Region condition because statement IDs do not have IAM resource ARNs. `GetCredentials` is restricted to the workgroup, and database grants restrict the actual SQL access. The Glue role receives procedure execution only; the dispatcher receives access only to the outbox.

## 8. Create the Lambda functions

For each row, open **Lambda → Create function → Author from scratch**, use **Python 3.12**, x86_64 architecture, and **Use an existing role**. Select the specified role.

| Function name | Source file | Execution role | Memory | Timeout |
|---|---|---|---|---|
| `mg-producer` | `lambda/producer.py` | `mg-producer-role` | 512 MB | 600 seconds |
| `mg-context` | `lambda/context_api.py` | `mg-context-role` | 256 MB | 30 seconds |
| `mg-cases` | `lambda/case_actions.py` | `mg-cases-role` | 256 MB | 15 seconds |
| `mg-dispatcher` | `lambda/outbox_dispatcher.py` | `mg-dispatcher-role` | 256 MB | 240 seconds |
| `mg-starter` | `lambda/workflow_starter.py` | `mg-starter-role` | 256 MB | 30 seconds |

For each function:

1. In the **Code** tab, open the default `lambda_function.py`, replace its entire contents with the matching source file, and choose **Deploy**.
2. Keep runtime handler **`lambda_function.lambda_handler`**. The repository filename does not determine the handler when its contents are pasted into `lambda_function.py`.
3. Under **Configuration → General configuration**, set memory and timeout from the table.
4. Leave VPC configuration unattached. Keep function URLs disabled; API Gateway is the HTTP entry point.
5. Under **Configuration → Environment variables**, add the values below.

| Function | Environment variables |
|---|---|
| `mg-producer` | `STREAM_NAME=mg-usage-stream`, `CONTRACT_TABLE=mg-contracts`, `RATE_TABLE=mg-rates` |
| `mg-context` | `CONTRACTS_TABLE=mg-contracts`, `RATES_TABLE=mg-rates` |
| `mg-cases` | `CASES_TABLE=mg-cases` |
| `mg-dispatcher` | `WORKGROUP_NAME=mg-workgroup`, `DATABASE=dev`; add `QUEUE_URL` in section 10. |
| `mg-starter` | Add `STATE_MACHINE_ARN` in section 11. |

Set reserved concurrency to **1** for `mg-producer`, **4** for `mg-context`, **1** for `mg-dispatcher`, and **2** for `mg-starter`, if the account quota permits. This bounds simultaneous producer invocations, lookup load, and dispatcher overlap. If the account cannot reserve concurrency, keep invocations controlled and request a quota adjustment before increasing load; logical duplicate handling remains necessary regardless. Run only one producer workflow at a time; reserved concurrency alone does not prevent two workflows from alternating their segments.

### Publish the producer version

1. Confirm the producer code, environment variables, 512 MB memory, and 600-second timeout are saved.
2. Open **Lambda → mg-producer → Versions → Publish new version**. Add a description such as `Initial sustained document-usage generator` and publish.
3. Record the numeric version as `PRODUCER_VERSION`. The producer workflow will invoke this exact version, whose code and environment are frozen. After a deliberate code/configuration correction, publish a new version and update the workflow before starting a new run identity.
4. Preserve the original version and all original input values for retries. Do not change generation logic or profiles under an existing `run_id`.

### Check the context handler

1. In `mg-context` → **Test → Create new event**, paste the `context_lambda` object from `config/invocations.json`.
2. Invoke it. Expect `statusCode: 200`, rate `0.40`, currency `INR`, and budget `500` inside the response body's `contexts` array.
3. A missing contract should return a context entry with `CONTRACT_NOT_FOUND`. A permission error or exhausted DynamoDB retry should fail the invocation rather than masquerade as a missing contract.

## 9. Add and secure the API methods

### Contract lookup route

1. Open **API Gateway → mg-api → Resources**. Under `/`, create resource `billing-context`, then child resource `batch`.
2. Select `/billing-context/batch` → **Create method → POST**.
3. Choose **Lambda function**, enable **Lambda proxy integration**, select `REGION`, and select `mg-context`.
4. Accept the permission prompt that allows API Gateway to invoke this Lambda. If it is not offered, open the Lambda's **Configuration → Permissions → Resource-based policy → Add permissions → AWS service → API Gateway**, grant `lambda:InvokeFunction` to `apigateway.amazonaws.com`, and use source ARN `arn:aws:execute-api:REGION:ACCOUNT_ID:API_ID/*/POST/billing-context/batch`. The stage wildcard includes the console's test-invoke stage; the method and resource stay restricted.
5. In **Method request → Edit**, set **Authorization: AWS_IAM**. An API key is not needed.

### Acknowledgement route

1. Under `/`, create `cases`, then child resource `{case_id}`, then child `ack`.
2. On `/cases/{case_id}/ack`, create **POST**, with Lambda proxy integration to `mg-cases`.
3. Allow API Gateway invocation and set **Authorization: AWS_IAM** on this method too.
4. For a manual resource permission, the source ARN is `arn:aws:execute-api:REGION:ACCOUNT_ID:API_ID/*/POST/cases/*/ack`; the lookup route uses `...:API_ID/*/POST/billing-context/batch`.

### Deploy and verify

1. Choose **Deploy API → New stage**, stage name `prod`.
2. Copy the invoke URL. The lookup URL is `https://API_ID.execute-api.REGION.amazonaws.com/prod/billing-context/batch`.
   Keep the stage spelling identical in the URL and IAM resource ARNs. These templates use lowercase `prod`. If the existing stage is `Prod`, preserve that capital P in the lookup URL and in the Glue role's `InvokeOnlyBillingContext` resource. Apply the same spelling to any operator acknowledgement permission. A permission for `/prod/POST/...` does not authorize `/Prod/POST/...`.
3. Under the lookup method's **Test** tab, paste `context_api_body` from `config/invocations.json` as the request body and invoke it. Expect HTTP 200 and the same rate snapshot.
4. The console method test is an administrative `TestInvokeMethod` operation. It validates integration, but does not prove a client's SigV4 permissions. The first successful Glue batch verifies the deployed IAM-authorized path.
5. In stage settings, configure a modest throttle, such as **20 requests/second** and **40 burst**. Glue uses at most four active enrichment partitions; monitor 429 responses before increasing concurrency.

Every method or authorization change requires another **Deploy API** to `prod`. Do not replace `AWS_IAM` with `NONE` to troubleshoot a signature error; check the URL, Region, role policy, and deployment first.

## 10. Create SNS and SQS

### SNS

1. Open **SNS → Topics → Create topic**, choose **Standard**, name `mg-budget-alerts`, and create it.
2. Record `TOPIC_ARN`. Keep access limited to the account and the workflow role's IAM permission.
3. Choose **Create subscription**, protocol **Email**, and enter an address you control. Confirm the subscription through the message sent by SNS.
4. Leave customer-managed topic encryption unconfigured for the supplied role policy. If enabled, grant the workflow service/role the corresponding KMS access before testing.

### SQS

1. Create a **Standard** queue `mg-alert-dlq` with **14-day message retention** and **SSE-SQS** encryption.
2. Create a **Standard** queue `mg-alert-queue` with **4-day retention**, **180-second visibility timeout**, **20-second receive wait time**, and **SSE-SQS** encryption.
3. In the main queue's **Dead-letter queue** settings, enable `mg-alert-dlq`, with **Maximum receives: 5**.
4. In the DLQ's redrive allow policy, allow redrive only from `mg-alert-queue`.
5. Copy the main queue URL into `mg-dispatcher`'s `QUEUE_URL` environment variable. A queue URL is different from its ARN.

The visibility timeout is six times the starter function's timeout. The workflow may run longer than the visibility timeout because the starter only waits for `StartExecution`, not for workflow completion.

## 11. Create the Step Functions workflows and SQS trigger

### Budget workflow

1. In CloudWatch Logs, create `/aws/vendedlogs/states/mg-budget-workflow`; set retention to **14 days**.
2. Open **Step Functions → State machines → Create state machine**. Choose a blank definition, use the **Code** editor, and choose **Standard** workflow type.
3. Paste `config/budget-workflow.asl.json`, replacing `REGION`, `ACCOUNT_ID`, and `TOPIC_ARN`. Keep the entire JSON object, including its `Comment` fields.
4. Name it `mg-budget-workflow`. Select existing role `mg-workflow-role`.
5. Enable logging to the log group above with level **ERROR** and execution data excluded. Keep the workflow timeout at 1,800 seconds. The acknowledgement wait is 120 seconds; change it to the intended operating interval when appropriate.
6. Create the state machine and record its ARN. Add that exact ARN to `mg-starter` as `STATE_MACHINE_ARN`.
7. Open **Lambda → mg-starter → Add trigger → SQS**, select `mg-alert-queue`, set **batch size 1** and **batch window 0**, and enable the trigger.
8. Edit the SQS event source mapping and enable **Report batch item failures** (`ReportBatchItemFailures`). This is required for the handler's returned failure list to cause retries. Set its **Maximum concurrency** to **2**, matching the starter's reserved concurrency when that reservation is available.

SQS confirms work once the Standard execution has started. A workflow that fails later is recovered from Step Functions, not from the already-acknowledged SQS message. Configure the failed-execution alarm in section 19 and follow the redrive procedure in section 18.

### Producer workflow

1. In CloudWatch Logs, create `/aws/vendedlogs/states/mg-producer-workflow` with **14-day** retention.
2. Open **Step Functions → State machines → Create state machine**, select a blank definition, **Standard** type, and the **Code** editor. The workflow uses JSONPath; keep the supplied `QueryLanguage` setting.
3. Paste the complete `config/producer-workflow.asl.json`, replacing `REGION`, `ACCOUNT_ID`, and `PRODUCER_VERSION`. For example, the function ARN ends in `:mg-producer:1` when the recorded version is 1. Leave its `Comment` fields intact.
4. Name the state machine `mg-producer-workflow`. Select existing execution role `mg-producer-workflow-role` and enable **ERROR** logs to the group above, excluding execution data.
5. Keep the state-machine timeout of **14,400 seconds** and Task timeout of **660 seconds**. The Lambda timeout is **600 seconds**, longer than its normal five-minute segment. The workflow supports a total workload duration up to 3,600 seconds, with additional headroom for retries.
6. Create it without starting an execution yet. Section 15 starts it after the pipeline is ready. It has no SNS or SQS trigger; the operator starts each bounded workload explicitly.

The workflow waits synchronously for each Lambda result, then uses its `next_input` only after a successful segment. A failed Task retries the same segment input up to twice. Retries can repeat transport deliveries; they do not assign new event identities. Missing references or invalid configuration eventually fail the workflow and remain visible in its execution history.

## 12. Create the independent raw archive

1. In **CloudWatch → Log groups**, create `/aws/kinesisfirehose/mg-raw-archive` and a log stream named `DestinationDelivery`; use 14-day retention.
2. Open **Amazon Data Firehose → Create Firehose stream**.
3. Choose source **Amazon Kinesis Data Streams**, select `mg-usage-stream`, and destination **Amazon S3**.
4. Name the delivery stream `mg-raw-archive` and select `BUCKET_NAME`.
5. Use S3 prefix `raw/!{timestamp:yyyy/MM/dd/HH}/` and error prefix `firehose-errors/!{firehose:error-output-type}/!{timestamp:yyyy/MM/dd/HH}/`.
6. Disable transformation, dynamic partitioning, and record-format conversion. The producer already emits newline-delimited JSON envelopes. Do not add a second wrapping transformation.
7. Set buffering hints to **1 MiB** and **60 seconds**, and compression to **GZIP**.
8. Use the bucket's SSE-S3 encryption. Select existing role `mg-firehose-role`.
9. Enable error logging to the log group/stream above. Create the delivery stream and wait until it is **Active** before producing usage.

The prefix uses the approximate arrival timestamp of the oldest record in each Firehose object, in UTC by default. It does not use the business event timestamp. Replay selects an arrival-time prefix and then filters events by their own timestamps. A late event can therefore be in a later S3 folder. Archive retries are bounded by Kinesis retention if S3 remains unavailable.

## 13. Initialize Redshift tables and procedure

1. Open **Redshift → Query Editor v2** and finish the editor's initial account setup if prompted. Use its default encryption option unless your organization requires a managed key.
2. Connect to `mg-workgroup`, database `dev`, as the namespace database administrator. The temporary application role users do not have schema-creation permission.
3. Paste and execute `sql/schema.sql`. If the editor runs one selected statement at a time, execute all statements in their displayed order.
4. The final `CREATE USER` statements create `IAMR:mg-glue-role` and `IAMR:mg-dispatcher-role`. On a second execution, skip those statements if the users already exist; do not replace them with password-authenticated application users.
5. Paste and execute `sql/commit_batch.sql` as that same administrator. This creates the default atomic `SECURITY DEFINER` procedure and grants the Glue role only `EXECUTE` on it.
6. Confirm the four tables and two views appear under schema `mg`. Run `SELECT * FROM mg.tenant_usage;` and expect no rows.
7. Open `mg-dispatcher` in Lambda and invoke it with `{}`. Expect `{"sent": 0}`. This checks its IAM workgroup credentials, database user mapping, outbox grants, and result retrieval before enabling its schedule.

Keep the commit procedure in its default atomic mode. `NONATOMIC`, an internal `COMMIT`, or `TRUNCATE` would break the intended transaction boundary. The associated COPY role must remain attached to the namespace. A serialization failure is safe to retry; it is not evidence that a batch should be marked complete.

## 14. Create the Glue job and dispatcher schedule

### Glue

1. Open **AWS Glue → ETL jobs → Script editor → Spark**, create a job, and name it `mg-usage-stream`.
2. In **Job details**, select role `mg-glue-role`, type **Spark Streaming**, Glue version **5.0**, language **Python**, worker type **G.1X**, and **2 workers**. The service command must be `gluestreaming` for the live job.
3. Set script location to `s3://BUCKET_NAME/scripts/usage_stream.py`. If the console uses an inline script editor, paste the same source and save it to that location.
4. Set maximum concurrent runs to **1**, retries to **0**, and disable job-run queuing. Set job timeout to **360 minutes** for a bounded initial deployment. A continuously operated service needs an explicit restart/maintenance policy using the same checkpoint.
5. Disable job bookmarks. Enable job metrics, observability metrics, and Spark UI logs; set Spark UI log location to `s3://BUCKET_NAME/spark-logs/`. Set any temporary directory to `s3://BUCKET_NAME/staged/glue-temp/`.
6. Leave Glue connections and VPC settings empty. The script reads Kinesis directly and uses HTTPS APIs, including the Redshift Data API; it does not need a JDBC connection or crawler.
7. Under **Advanced properties → Job parameters**, add every `--...` entry from `config/glue-parameters.json`, replacing placeholders. Its `_instructions` and `_parameter_help` entries are documentation; do not add them as job arguments.
8. Keep `--GENERATION=live-001`, `--PROCESSING_VERSION=v1`, `--RUN_MODE=live`, and `--FAIL_AFTER_COMMIT_BATCH=-1`. All three unused replay arguments must still exist with value `unused`.
9. Keep the pinned `--additional-python-modules` value. It supplies a boto3/botocore version that supports conditional S3 `IfNoneMatch` writes; older built-in SDK versions can reject this argument. Dependency resolution requires access to the public package index from the managed runtime.
10. Save the job but do not run it until the stream/archive/API/warehouse checks above are complete.

### Dispatcher schedule

1. Open **Amazon EventBridge → Rules → Create rule**, name it `mg-dispatch-outbox`, and select a **Schedule** rule on the default event bus. Use the scheduled-rule path for this configuration.
2. Set a rate expression of **1 minute**.
3. Select target **AWS service → Lambda function → mg-dispatcher**, with constant JSON input `{}`.
4. Allow the console to add Lambda's resource-based invocation permission for `events.amazonaws.com`, restricted to this rule ARN. If it is not added, add it under the function's **Configuration → Permissions**.
5. Create the rule enabled. The dispatcher polls at most ten pending alerts per invocation. Increase throughput only after measuring database latency and queue consumption.

Scheduled rules and EventBridge Scheduler have different permission models. If choosing Scheduler instead, create its Lambda-invocation execution role and set flexible time window off; do not reuse the rule's resource-policy-only instructions.

## 15. Verify the complete path

### Understand the two inputs

**Reference data** consists of the customer contracts and rate cards entered into DynamoDB in section 3. Those records tell the lookup API how to price a particular customer's usage at a particular contract version. They are not the usage stream.

**Usage data** is generated by `lambda/producer.py`, deployed as `mg-producer`. This implementation creates synthetic records representing completed document-processing operations. It does not need PDF uploads, a source CSV, a running document-extraction application, or DynamoDB Streams. In an application integration, the document-processing backend would emit this same event contract after completing each request.

The sustained source path is:

```text
Step Functions execution input
    -> mg-producer-workflow invokes five-minute segments
    -> mg-producer creates individual usage events
    -> Kinesis receives the events
        -> Glue validates, enriches through the API, and commits to Redshift
        -> Firehose independently archives the original events in S3
```

API Gateway is used by Glue for enrichment; the producer publishes directly to Kinesis. Start with the small Lambda inputs below to check exact totals quickly, then run the sustained workload in this same section. Count mode's fixed 20-page operations are confined to those small acceptance inputs.

### Configure a producer invocation

Paste the following object into the **mg-producer** Lambda console's test-event editor. It is the `producer_initial` object in `config/invocations.json`, reproduced here for clarity. Do not paste the entire configuration file or its documentation entries.

```json
{
    "run_id": "baseline-001",
    "count": 100,
    "duplicate_every": 10,
    "start_time": "2026-10-18T10:00:00Z",
    "tenant_id": "customer-017",
    "contract_version": "v1"
}
```

| Input | What it controls |
|---|---|
| `run_id` | Identifies a set of operations. Use the identical input to retry that set; choose a new ID for new operations. |
| `count` | Number of distinct operations to generate, from 1 to 10,000. Each currently represents 20 successfully processed pages. |
| `duplicate_every` | Repeat each zero-based sequence index divisible by this number. With 100 operations and value 10, indexes 0, 10, …, 90 repeat, adding ten deliveries. Zero disables deliberate duplicates. |
| `start_time` | Timestamp of the first operation. Each following operation adds one second. This is business event time, not the invocation date or send rate. |
| `tenant_id` | Customer whose usage is being generated; also the Kinesis partition key. It must match a seeded contract unless checking missing-reference handling. |
| `contract_version` | Exact contract version carried by every generated event in this invocation. Glue uses it to fetch the historical rate. |

### Inspect an individual generated event

The invocation object above controls generation. It is different from the individual business events sent to the stream. Its first generated business event is:

```json
{
    "event_id": "ccbd4b29e6d702ef63b90a05e38ef5c1d7a3c0687ee141ba6f086db104e057dc",
    "tenant_id": "customer-017",
    "request_id": "baseline-001-0",
    "event_time": "2026-10-18T10:00:00Z",
    "service": "document_extraction",
    "pages_processed": 20,
    "outcome": "SUCCESS",
    "contract_version": "v1"
}
```

`event_id` is the SHA-256 hash of `customer-017:baseline-001:0`. It remains identical on a retry. `request_id` is a readable operation identifier. Subsequent events use sequence indexes 1 through 99, ending at `10:01:39Z`. Prices, currency, budget, and calculated charge are added later through the API lookup and Glue transformation.

For transport, the producer serializes each business event into a string-valued `payload` field inside an outer JSON object. It appends a newline and sends those bytes using Kinesis `PutRecords`, with the customer as `PartitionKey`. Firehose preserves these envelopes as newline-delimited JSON in compressed S3 objects. When inspecting raw data, parse the outer object and then parse its `payload` string to see the fields above.

### Start processing and publish the initial events

1. In Glue, run `mg-usage-stream`. Open its **Runs** tab, then the run's CloudWatch logs. Wait for the streaming query to initialize; provisioning can take several minutes.
2. In **Lambda → mg-producer → Test**, create a private test event. Paste only the `producer_initial` object from `config/invocations.json` and invoke it once.
3. Expect a return object containing `unique_operations: 100`, `delivered_records: 110`, `expected_pages: 2000`, and `run_id: baseline-001`. This confirms Kinesis accepted the records, not that Redshift has committed them yet. The 2,000 pages count distinct operations; duplicate deliveries do not represent extra work.
4. Watch the Glue output log for a JSON line with `"status": "COMMITTED"`. If the job fails, inspect its error before submitting more events.
5. In Query Editor v2, run the following query. Continue only when the expected row appears.

```sql
SELECT *
FROM mg.tenant_usage
WHERE tenant_id = 'customer-017'
    AND billing_period = '2026-10-01'
    AND processing_version = 'v1';
-- Expected: operations=100, billable_pages=2000,
-- currency=INR, estimated_total=800.000000.
```

6. Run the duplicate and reconciliation queries in `sql/reports.sql`. The duplicate-key queries must return no rows. Every committed batch must have `input_count = valid_count + rejected_count`; `inserted_count` can be lower because duplicates are legitimate input.
7. In S3, confirm objects exist under `staged/live-001/`, `checkpoints/live-001/spark/`, and `raw/`. Firehose buffering means raw objects can appear later than facts. Wait for an archived GZIP object before attempting replay. Download one through S3 and inspect it locally if needed: each line must contain a JSON object with a string-valued `payload`.
8. Inspect `mg.alert_outbox`. There should be one case for this customer/month/version/budget policy. Its `estimated_total` is the total at the first committed threshold crossing, not a continuously updated balance.
9. Within the dispatcher interval plus service startup time, `sent_at` should become non-null. In Step Functions, an execution named with that row's `case_id` should start. In DynamoDB, `mg-cases` should contain the same ID. Confirm the initial SNS notification reaches the subscribed address.

### Acknowledge a case, or let it escalate

1. Copy `case_id` from `mg.alert_outbox` or the workflow input.
2. In **API Gateway → mg-api → Resources → /cases/{case_id}/ack → POST → Test**, supply that ID as the path parameter and `{}` as the body. Invoke it while the execution is in its 120-second wait.
3. Expect HTTP 200 and `ACKNOWLEDGED`. Refresh the DynamoDB item and verify `acknowledged_at`. Repeating the same acknowledgement preserves its first timestamp.
4. After the wait, an acknowledged case finishes without escalation. An unacknowledged case sends an escalation notification and records `ESCALATED`. A late acknowledgement is accepted; it cannot retract an email already sent.
5. For a deployed HTTP client, use SigV4 with service `execute-api`, the selected Region, and an identity allowed by `operator-api-invoke-policy` in the IAM file. The console method test uses your administrative permission and does not exercise this client authorization path.

Email delivery can repeat. Check logical case identity and warehouse totals rather than asserting that exactly one email was received. The case-status read and SNS publication are separate operations; an acknowledgement racing an escalation can still receive that escalation email, while the conditional DynamoDB update preserves `ACKNOWLEDGED`.

### Check duplicate delivery and a rate change

1. Invoke **the exact same** `producer_initial` input again. After the next commit, the totals must remain 100 operations, 2,000 pages, and INR 800. The outbox still has one logical case.
2. Invoke `producer_after_rate_change`. These are new event IDs under contract `v2`, which points to the INR 0.30 rate.
3. After commit, expect **200 operations, 4,000 billable pages, and INR 1,400** for October `v1`. Here `v1` is the processing version; individual facts can carry contract versions `v1` or `v2`.
4. Inspect a fact from each contract version. The earlier events must retain `0.400000`; later events use `0.300000`. There is still one monthly budget case because the budget policy is unchanged.
5. Invoke `producer_missing_reference`. Expect a quarantine object with reason `CONTRACT_NOT_FOUND`, a committed batch with a rejected record, and no change to the customer totals above.

The fixed event dates remain valid when execution occurs on another date. Producer identity is derived from customer, `run_id`, and sequence number. Never reuse a `run_id` with changed event parameters: that represents conflicting versions of an existing operation.

### Publish the sustained workload

The sustained workload runs on AWS for **at least 15 minutes** and targets **200 distinct operations per second**. It produces **180,000 distinct operations**, with **1,800** additional intentional duplicate deliveries. It uses four separate customers, so it does not change the `customer-017` acceptance totals used in sections 16–17.

A standard on-demand Lambda invocation has a [900-second maximum timeout](https://docs.aws.amazon.com/lambda/latest/dg/configuration-timeout.html). The producer workflow therefore uses three sequential five-minute segments with a 600-second function timeout. This leaves room for network delays and retries. The total run includes 900 seconds of paced generation plus startup, transitions, preflight reads, and any retry delays. Expect small gaps between segments; publication is not an uninterrupted socket stream.

1. Confirm all eight DynamoDB seed items exist, Kinesis and Firehose are active, and the live Glue job is running with `--FAIL_AFTER_COMMIT_BATCH=-1`. Confirm the dispatcher schedule is enabled.
2. Open **Step Functions → State machines → mg-producer-workflow → Start execution**. Use a descriptive execution name such as `usage-20261018-001-first`.
3. Paste **only** `producer_sustained` from `config/invocations.json` into the execution input:

```json
{
    "mode": "sustained",
    "run_id": "usage-20261018-001",
    "duration_seconds": 900,
    "events_per_second": 200,
    "duplicate_every": 100,
    "start_time": "2026-10-18T14:00:00Z"
}
```

4. Start the execution once. The execution graph should remain in `ProduceSegment` for roughly five minutes, pass through `WorkloadComplete` and `UseNextSegmentInput`, and repeat until all three segments finish. Do not launch another producer workflow concurrently.
5. Open **Lambda → mg-producer → Monitor → View CloudWatch logs**. Every 30 generated seconds, a progress line includes `run_id`, `completed_seconds`, `segment_delivery_records`, and `target_events_per_second`. Delivery counts in each line are local to that segment; `completed_seconds` advances across the whole run.
6. In Kinesis monitoring, inspect **IncomingRecords**, **IncomingBytes**, and **WriteProvisionedThroughputExceeded**. The normal input is about **12,120 delivery records per minute**, including intentional duplicates. Initial/final partial metric windows, segment gaps, or retries can change individual minute totals.
7. In Glue, watch successful `COMMITTED` batches and processing lag. In S3, verify raw archive and staged prefixes continue receiving objects. The producer's success confirms Kinesis acceptance; it does not mean Redshift has caught up yet.
8. On workflow success, inspect its output. Expect `complete=true`, `completed_seconds=900`, `unique_operations=180000`, and `planned_delivery_records=181800`. The `segment` object describes only the final five minutes. Unknown network outcomes and segment retries can add physical deliveries beyond the planned figure; do not treat it as an exact count of all records written during failures.
9. Wait for Glue to catch up, then run the query below. Each customer must have 45,000 facts for this run and processing version, regardless of duplicate delivery. Use the exact `run_id` in the ID prefix if the input was changed.

```sql
SELECT
    tenant_id,
    COUNT(*) AS operations,
    SUM(CASE WHEN outcome = 'FAILED' THEN 1 ELSE 0 END) AS failed_operations,
    SUM(pages_processed) AS billable_pages,
    SUM(estimated_charge) AS estimated_total
FROM mg.usage_fact
WHERE processing_version = 'v1'
    AND event_id LIKE 'USG-usage-20261018-001-%'
GROUP BY tenant_id
ORDER BY tenant_id;
```

For the unchanged supplied input and generator, expect:

| Customer | Operations | Failed operations | Billable pages | Estimate, INR |
|---|---:|---:|---:|---:|
| `aster-lending` | 45,000 | 1,103 | 1,162,072 | 464,828.80 |
| `harbor-claims` | 45,000 | 1,060 | 593,109 | 237,243.60 |
| `cobalt-logistics` | 45,000 | 1,060 | 198,142 | 79,256.80 |
| `maple-accounts` | 45,000 | 1,045 | 131,460 | 52,584.00 |

These are synthetic usage charge estimates, not AWS infrastructure charges. The four customers cross their configured budgets during this workload, so expect four additional logical budget cases and their asynchronous notifications. Repeating the same workload must preserve these facts, totals, and case identities.

### What makes the generated data valid

| Customer | Document category | Successful pages/request | Monthly budget, INR |
|---|---|---|---:|
| `aster-lending` | Loan application packets | 8–45 | 75,000 |
| `harbor-claims` | Insurance claim packets | 3–24 | 30,000 |
| `cobalt-logistics` | Shipment documents | 1–8 | 20,000 |
| `maple-accounts` | Invoices | 1–5 | 15,000 |

The names and distributions are fictional workload definitions. They do not represent measured customer traffic or contain personal information. All records use `service=document_extraction`, a real seeded `(tenant_id, contract_version)` pair, timezone-aware timestamps, and bounded integer page counts. Approximately 2% of operations have `outcome=FAILED` and zero processed pages; Glue therefore assigns them zero charge while retaining the failed operation as a fact.

The producer checks the shared INR 0.40 rate and all four contracts before writing a segment, including whether the entire requested event-time interval fits the contracts. Missing or incompatible setup fails the invocation before that segment publishes anything. The Glue job independently validates and enriches every event through the lookup API.

The first business event of the supplied sustained input is:

```json
{
    "event_id": "USG-usage-20261018-001-000000000",
    "tenant_id": "aster-lending",
    "request_id": "REQ-APPLICATION-usage-20261018-001-000000000",
    "event_time": "2026-10-18T14:00:00Z",
    "service": "document_extraction",
    "pages_processed": 10,
    "outcome": "SUCCESS",
    "contract_version": "v1"
}
```

IDs contain the run identity and a global sequence that continues across invocations. The request ID also contains its document category. Values are derived deterministically from that identity; retries do not create new random documents. At 200 operations/second, business timestamps advance by five milliseconds, ending at `2026-10-18T14:14:59.995Z`. The fixed start time supports repeatability; it does not schedule execution for that date. Publication happens in paced one-second groups.

### Change volume or recover a producer run

- **New workload:** choose a fresh `run_id`. `duration_seconds` supports 1–3,600 seconds and `events_per_second` supports 1–250. For example, 3,600 seconds at 200 operations/second creates **720,000** distinct operations. `duplicate_every=0` disables deliberate duplicate delivery. The duration/rate settings are for sustained mode; the count inputs retain their original behavior.
- **Capacity:** the default is about 202 delivery records/second, roughly 6,060 records per nominal 30-second Glue window. One provisioned Kinesis shard supports [1,000 records/second and 1 MB/second writes](https://docs.aws.amazon.com/streams/latest/dev/service-sizes-and-limits.html). This establishes ingestion headroom, not a measured end-to-end throughput guarantee. Monitor actual bytes, Glue batch duration/lag, API throttling, and Redshift commit latency. Heavy duplication or slow commits can exceed the consumer's sustained processing rate. Lower the producer rate or tune consumer capacity from observed metrics; do not assume adding shards fixes API or warehouse bottlenecks.
- **Automatic retry:** `publish` retries individual rejected Kinesis entries. If the Lambda invocation fails, Step Functions retries its complete original segment. Stable IDs and payloads make those repeated deliveries safe for the warehouse commit procedure.
- **Failed execution:** inspect its failed Task input and Lambda logs; repair IAM, reference data, or capacity. Use Step Functions **Redrive** when eligible. Do not edit the run settings or generator version. A restart with a new execution name and the identical original workload input also reproduces the complete operation set safely, but sends all 180,000 operations again.
- **Stop:** use **Stop execution** on the producer workflow to prevent subsequent segments. Stopping a Step Functions execution does not cancel an already running Lambda invocation; that segment can continue publishing until it finishes or hits its timeout. Leave downstream processing active while it drains. An incomplete run is not a successful 180,000-operation run; restart with identical input to finish safely.
- **Continuation state:** `start_second` in `next_input` is internal workflow progress. Do not invent or edit it. It advances only after all records in the segment were accepted. For archive replay, use section 17 instead of regenerating data.

All remaining fixed 100/200/210-operation acceptance totals refer specifically to `customer-017`, October 2026, processing version `v1`. The four sustained customers are additional warehouse facts. Stop the producer workflow and allow any active segment to finish before the fault-injection or replay steps below.

## 16. Verify fault recovery

### Crash after commit, before checkpoint completion

This check uses the existing live generation and an explicit failure switch. It does not delete any checkpoint.

1. Wait for section 15's inputs to commit. Stop the Glue run and wait until its status is **Stopped**.
2. Edit the job parameter `--FAIL_AFTER_COMMIT_BATCH` to `any`. Leave `--GENERATION=live-001` and all other parameters unchanged. Save and run the job again.
3. Invoke `producer_recovery` once. It contains ten distinct operations, each delivered twice.
4. The next nonempty committed batch intentionally raises `Injected failure AFTER Redshift commit and BEFORE checkpoint completion`. The job must fail. An older uncompleted batch may trigger the switch first; in that case restore and drain it, then repeat this sequence for the new input.
5. Inspect `mg.batch_commits` and the Glue `COMMITTED` log entry. Record the generation, batch ID, manifest URI, and current October total. When all ten new events were in that committed batch, the cumulative total is **210 operations, 4,200 pages, and INR 1,480**. If the source split them across batches, the intermediate total can be lower.
6. Change only the failure parameter back to `-1`. Restart the same job with the same generation and checkpoint.
7. The failed batch reuses its existing `ready.json` and manifest. The procedure finds its matching ledger entry and returns without inserting its facts again. Remaining batches then finish normally.
8. Verify the final cumulative total is exactly **210 operations, 4,200 pages, and INR 1,480**, with no duplicate fact or ledger keys. This result must hold regardless of micro-batch boundaries.

The failure switch is deliberately excluded from the generation identity so it can be disabled for recovery. Never leave it set to `any` during ordinary operation.

### Temporary context API failure

1. After the stream has caught up, open `mg-context` in Lambda and temporarily set **Reserved concurrency: 0**. Record its previous setting first. This blocks new lookup invocations.
2. Invoke `producer_initial` again with the identical body. New stream deliveries still require enrichment unless their exact batch already has a ready manifest.
3. Glue retries the API call for throttling/service failures. Exhausted retries fail the batch and job. The outage must not be recorded as `CONTRACT_NOT_FOUND` or as a successful batch of rejected usage.
4. Check that Firehose continues archiving the stream independently. Check that no new fact keys or increased charge totals appeared.
5. Restore the Lambda concurrency setting. Restart Glue with the same generation. The backlog is processed and duplicate facts remain suppressed.

If the account cannot allocate reserved concurrency, perform the same check by temporarily removing `execute-api:Invoke` from **only** `mg-glue-role`, then restoring that exact policy. An HTTP 403 fails immediately rather than entering the transient retry loop. Do not change API authorization to public access.

### Invalid or conflicting input

Missing references and invalid business payloads are durable rejects: they go to S3 quarantine and the batch records their count. After adding a genuinely missing reference version, replay the original raw events with a new replay generation; the same already-completed live batch is not re-enriched.

An existing event key with a different payload or price is different: `commit_batch` raises an error and the entire transaction rolls back. To verify this on a disposable input, publish one fresh `run_id`, let it commit, then submit that same `run_id` with a different `start_time` inside its valid contract period. Expect an `Existing event has a different payload or pricing snapshot` error, no new ledger row for that failed batch, and unchanged committed totals. The incompatible record remains in the stream; restarting cannot resolve a genuine identity conflict. Investigate the producer, preserve the conflicting raw record, and use an explicitly reviewed recovery selection/new generation. Do this check after the ordinary recovery/replay checks, because it deliberately stops that source generation.

## 17. Replay the S3 archive

### Select and freeze the input

1. Stop the live Glue job and wait until it is **Stopped**. Record its generation `live-001` and processing version `v1`.
2. Stop producing new events for the bounded replay selection. Wait until Firehose has delivered the selected records, its delivery-freshness metric has caught up, and no delivery errors remain.
3. In S3, select an arrival-time prefix containing the required objects, such as `s3://BUCKET_NAME/raw/2026/10/18/`. Use the actual folder created by Firehose, even if it differs from the event date. A broader day/month prefix is appropriate when late arrivals must be included.
4. Record the object list and the expected unique event count. Use only a fully delivered, stable selection for one replay generation. A growing prefix is not a frozen snapshot; for a later expanded selection use another generation. The ready manifest freezes enriched output once it is published, not the raw input before that point.
5. Record the current October fact count, total, and outbox count. For all preceding supplied inputs, expect 210 facts and INR 1,480 for `customer-017`/October/processing `v1`.

### Create and run the bounded replay job

1. Create another Glue **Spark** job named `mg-usage-replay`, using the same script, role, Glue 5.0 runtime, two G.1X workers, logs, and dependency parameters as the live job.
2. Select ordinary **Spark** job type (`glueetl`), not Spark Streaming. Set maximum concurrent runs 1, retries 0, and a bounded timeout such as 60 minutes.
3. Copy all application arguments from the live job, then change the following values:

| Parameter | Replay value |
|---|---|
| `--RUN_MODE` | `replay` |
| `--GENERATION` | `replay-001` |
| `--PROCESSING_VERSION` | `v1` |
| `--REPLAY_PREFIX` | The actual `s3://BUCKET_NAME/raw/.../` prefix selected above. |
| `--REPLAY_FROM` | `2026-10-18T00:00:00Z` |
| `--REPLAY_TO` | `2026-10-19T00:00:00Z` |
| `--FAIL_AFTER_COMMIT_BATCH` | `-1` |

4. Keep the other arguments, including stream ARN, API URL, SDK dependencies, and `--conf=spark.speculation=false`. Replay still calls the versioned lookup API, stages files, and invokes the same commit procedure.
5. Run the job. It recursively reads the chosen raw prefix, filters by the half-open event-time range `[REPLAY_FROM, REPLAY_TO)`, processes one bounded batch, and exits. Records with an unreadable business event time remain selected so validation can quarantine them. A malformed outer transport envelope fails the replay read.
6. On success, query the ledger for `generation = 'replay-001'`. Expect batch ID 0, `publish_alerts=false`, and `inserted_count=0` when the selected valid events already exist. Input counts include archived duplicate deliveries and may exceed fact counts.
7. Re-run the same replay generation with the same completed input. It reuses its staged output and ledger identity. Facts, totals, and outbox row count must remain unchanged.
8. Stop the replay job if still running. Resume the live job with its **original** generation `live-001`, original live parameters, failure switch `-1`, and intact checkpoint.

Replaying valid events missing from the warehouse inserts those events once. It deliberately creates no new alert intent. Previously committed pending outbox rows may still be delivered by the dispatcher; that is independent of replay. If strict historical-notification silence is needed during reconciliation, disable the dispatcher rule and wait for existing workflow executions to settle, then restore the rule afterward.

### Changed logic and lost checkpoints

- To evaluate changed processing logic, use a new generation and new `PROCESSING_VERSION`, and increment the script's `CODE_VERSION` when changing transformation behavior. Facts for different processing versions coexist; compare them explicitly and choose which version reports use. This does not implement automatic invoice correction or automatic replacement of historical facts.
- Contract/rate versions remain immutable even for recalculation. A new processing version alone does not authorize rewriting old DynamoDB reference items.
- If new objects arrive after a replay generation has completed, create a new replay generation for the expanded selection. Reusing `replay-001` would correctly reuse its already-frozen manifest, not discover additional events.
- If a live checkpoint is lost, do not reuse its generation. The script rejects a used generation whose Spark metadata is missing. Restore the checkpoint from a verified backup, or reconcile the missing interval from the raw archive, then start a new live generation. The new live job begins at the oldest retained Kinesis records; existing fact keys suppress overlap.
- If stream retention has expired, do not simply turn off `failOnDataLoss`. First identify and replay the missing archived interval, reconcile its counts, and only then start a new generation. A period missing from both Kinesis and S3 cannot be recovered by this project.

## 18. Recover notification processing

### Pending outbox or duplicate dispatch

If `sent_at` is null, check dispatcher logs, its Redshift identity, queue URL, and IAM permissions. Invoke it with `{}` after fixing the error or let the enabled schedule retry. A successful SQS send followed by a failed database update can send the same case again. Never manually set `sent_at` to bypass an unresolved send failure.

To verify this recovery boundary without altering warehouse facts, disable the dispatcher rule before producing usage for a new billing month, wait for the outbox row, and confirm it remains pending. Re-enable the rule and verify it is delivered. Use a new producer `run_id`, a timestamp in the new month, and a contract valid at that timestamp; contract `v2` covers months after October 2026. This is a separate month and does not change the October acceptance totals.

### SQS dead-letter queue

1. In **SQS → mg-alert-dlq → Send and receive messages**, inspect the failed case message without deleting it.
2. Read the corresponding `mg-starter` CloudWatch error. Fix missing IAM permissions, an incorrect state-machine ARN, or a malformed input at its source.
3. If the execution name already exists, inspect that exact Standard execution. Matching `RUNNING` or `SUCCEEDED` executions are safe duplicates; mismatched input is a conflict. Failed, aborted, or timed-out executions are intentionally not treated as success.
4. For a failed execution that supports redrive, fix its failing integration and use **Step Functions → Execution → Redrive**. Track it until it succeeds.
5. Then use **SQS → mg-alert-dlq → Start DLQ redrive**, select the source queue and a low rate. The starter will recognize the now-successful execution and acknowledge the repeated message.
6. Confirm the DLQ is empty and no new failed receives appear. Redrive requires the console operator's SQS permissions, which are separate from execution-role permissions.

If Step Functions no longer permits redrive, retain the failed message and case for an explicit recovery decision. The automatic path deliberately does not bypass a failed named execution with a random new ID. Recovery beyond the service's redrive window requires an operator-approved replacement execution and reconciliation of the original queue message; do not erase the case or outbox history to force a resend.

### Workflows that fail after starting

A successful `StartExecution` allows SQS to delete the message. A later workflow failure therefore has no automatic SQS redrive. Use the Step Functions failed/timed-out execution alarm, inspect the failed state, repair its dependency, and redrive the execution when eligible. Inspect `mg-cases` before manually repeating a notification. SNS retries and execution redrive may send another email.

## 19. Configure monitoring and retention

Set a finite CloudWatch log retention, such as 14 days, for all `/aws/lambda/mg-*` groups and Glue log groups created by the first run. Keep event payloads out of routine logs; the intentionally retained raw archive and quarantine contain the original usage records.

For each alarm below, open **CloudWatch → Alarms → Create alarm → Select metric**, choose the indicated resource dimension, set the statistic/period/threshold, and send alarm actions to `mg-budget-alerts`. Give each alarm a name starting with `mg-`. Use a missing-data treatment appropriate to the metric; sparse failure metrics can treat missing data as not breaching. Verify subscriptions are confirmed. These thresholds are initial operating values, not throughput guarantees.

In **SNS → mg-budget-alerts → Edit → Access policy**, retain the existing policy and append this object inside its `Statement` array, replacing placeholders. It allows the project's CloudWatch alarms to publish operational notifications:

```json
{
    "Sid": "AllowMeterGuardAlarmNotifications",
    "Effect": "Allow",
    "Principal": {
        "Service": "cloudwatch.amazonaws.com"
    },
    "Action": "sns:Publish",
    "Resource": "arn:aws:sns:REGION:ACCOUNT_ID:mg-budget-alerts",
    "Condition": {
        "ArnLike": {
            "aws:SourceArn": "arn:aws:cloudwatch:REGION:ACCOUNT_ID:alarm:mg-*"
        },
        "StringEquals": {
            "aws:SourceAccount": "ACCOUNT_ID"
        }
    }
}
```

| Signal | Initial alarm configuration | Response |
|---|---|---|
| Lambda `Errors` for dispatcher and starter | `AWS/Lambda`, FunctionName, Sum ≥ 1, 1-minute period | Inspect logs; pending outbox or queue items should remain recoverable. |
| Lambda `Errors` and `Throttles` for producer | `AWS/Lambda`, FunctionName=`mg-producer`, Sum ≥ 1, 1-minute period | Check the failed segment, seeded references, stream capacity, and overlapping executions. |
| Lambda `Throttles` for context | `AWS/Lambda`, FunctionName=`mg-context`, Sum ≥ 1, 1-minute period | Check reserved concurrency and API request pressure. |
| API Gateway `5XXError` | `AWS/ApiGateway`, ApiName=`mg-api`, Sum ≥ 1, 1-minute period | Restore lookup availability and restart failed Glue work. |
| SQS `ApproximateAgeOfOldestMessage` | `AWS/SQS`, QueueName=`mg-alert-queue`, Maximum > 300 seconds, 1-minute period | Check starter invocation and permissions. |
| SQS `ApproximateNumberOfMessagesVisible` | `AWS/SQS`, QueueName=`mg-alert-dlq`, Maximum ≥ 1, 1-minute period | Inspect and redrive after repair. |
| Step Functions `ExecutionsFailed` and `ExecutionsTimedOut` | Two alarms per state machine, `AWS/States`, StateMachineArn, Sum ≥ 1, 1-minute period | Recover failed producer segments; recover budget workflows even if the original SQS message is gone. |
| Firehose `DeliveryToS3.DataFreshness` | `AWS/Firehose`, DeliveryStreamName=`mg-raw-archive`, Maximum > 300 seconds, 1-minute period | Repair archive delivery before Kinesis retention is exhausted. |

Also create a Glue failure notification rule:

1. In **EventBridge → Rules → Create rule**, choose an **event pattern** on the default bus and name it `mg-glue-failures`.
2. In the pattern's JSON editor, paste:

```json
{
    "source": [
        "aws.glue"
    ],
    "detail-type": [
        "Glue Job State Change"
    ],
    "detail": {
        "jobName": [
            "mg-usage-stream",
            "mg-usage-replay"
        ],
        "state": [
            "FAILED",
            "TIMEOUT"
        ]
    }
}
```

3. Target the SNS topic `mg-budget-alerts`. Use its resource-policy permission option and allow the console to add the `events.amazonaws.com` publisher. If it does not, append the following statement to the topic's existing `Statement` array. If the console instead creates a target execution role, inspect that role and restrict its permission to `sns:Publish` on this topic.

```json
{
    "Sid": "AllowEventBridgeFailureNotifications",
    "Effect": "Allow",
    "Principal": {
        "Service": "events.amazonaws.com"
    },
    "Action": "sns:Publish",
    "Resource": "arn:aws:sns:REGION:ACCOUNT_ID:mg-budget-alerts"
}
```

4. Verify the injected failure from section 16 produces an operational notification after this rule is enabled. Create this rule before that check if immediate coverage is required.

In Glue's run metrics, watch processing duration against the 30-second trigger interval and consumer lag against 72-hour stream retention. A batch taking longer than its trigger interval does not overlap itself; it increases lag. For Kinesis, inspect write/read throttling and iterator age for both consumers. For Redshift, inspect failed queries, queue time, and capacity usage.

Run the pending-outbox query from `sql/reports.sql` during operational checks. A nonempty backlog with a growing oldest-pending age is actionable even if Lambda did not emit a fresh error. Periodically reconcile selected raw archive intervals against accepted/rejected counts and distinct fact keys; raw delivery counts alone include duplicates and cannot be compared directly to unique facts.

## 20. Troubleshooting

| Symptom | Check and corrective action |
|---|---|
| API returns 403 | Deployed stage and path, `AWS_IAM`, SigV4 Region/service, current temporary credentials, and Glue's exact `execute-api:Invoke` ARN. Redeploy method changes. |
| `CONTRACT_NOT_FOUND` or `RATE_NOT_FOUND` | Exact DynamoDB key strings and reference items. Add the missing immutable version, then replay the rejected raw event with a new generation. |
| Lambda receives DynamoDB `UnprocessedKeys` repeatedly | Capacity/throttling and permissions. Exhaustion must fail the API; it must not become a missing reference. |
| Data API denies credentials | Workgroup **ID** in IAM versus **name** in environment/arguments; Region; `redshift-serverless:GetCredentials`; IAM-mapped database users and SQL grants. |
| COPY cannot read a file | Namespace-associated COPY role, staged-prefix policy, manifest URLs, mandatory files, same Region, and enhanced-VPC-routing settings. Inspect Redshift query/COPY error details. |
| `Unknown parameter ... IfNoneMatch` | Restore the pinned boto3/botocore arguments and confirm Glue dependency installation succeeded. |
| `Generation configuration changed` | A generation was reused with another mode/version/API/destination/replay range or code version. Restore its original configuration or deliberately allocate a new generation. |
| `Used generation has lost its Spark checkpoint` | Restore the checkpoint or follow archive reconciliation/new-generation recovery. Never remove this guard to recycle batch IDs. |
| Procedure reports conflicting duplicates | Compare `source_hash`, `result_hash`, and immutable reference versions. Do not overwrite historical facts or edit the ledger to suppress the error. |
| Redshift serialization failure | Leave checkpoint and manifest intact and retry the same generation after other work settles. All writers must use the commit procedure. |
| Replay finds no input | Actual Firehose arrival-date folder, archive delivery completion, recursive input path, and event-time bounds. A replay of an empty selection creates no ledger row. |
| Queue messages repeat | Enable partial batch responses, inspect Lambda errors, check visibility timeout, and inspect the named workflow's status. Duplicate delivery itself is expected. |
| No email despite `sent_at` | That field only confirms SQS acceptance. Inspect the workflow, case state, SNS subscription confirmation, and mail filtering. |

For a timeout with an uncertain warehouse result, restart with the same generation and manifest. A successful earlier commit will be recognized; an uncommitted attempt will run again. Do not infer failure solely because the client did not receive the final success response.

### Resolve an HTTP 403 from the context lookup

An executor message such as `Stage cancelled` can follow an earlier request failure. Find the first Python exception. `Context API failed with HTTP 403` identifies the Glue-to-API lookup; the batch has not completed enrichment or reached its warehouse commit.

1. In **Glue → Job → Job details**, record the actual IAM execution role. In the failed run's arguments, record `--CONTEXT_URL` and `--REGION`. The lookup URL must include the API ID, deployed stage, and complete `/billing-context/batch` path. The signing Region must match the API's Region.
2. Open **IAM → Roles → that execution role → Permissions**, then edit the policy containing `InvokeOnlyBillingContext`. Match its `execute-api:Invoke` resource to the actual API, account, Region, stage, verb, and path. Preserve the role's other permissions. For a stage named `Prod`, the statement is:

```json
{
    "Sid": "InvokeOnlyBillingContext",
    "Effect": "Allow",
    "Action": "execute-api:Invoke",
    "Resource": "arn:aws:execute-api:REGION:ACCOUNT_ID:API_ID/Prod/POST/billing-context/batch"
}
```

3. In **API Gateway → API → Stages**, confirm that exact stage exists and its deployment includes `POST /billing-context/batch`. Under the resource's method request, keep **Authorization: AWS_IAM** and **API key required: false**. Deploy method changes to the existing stage. Editing only the IAM role policy requires no API redeployment.
4. The updated Glue script reports `error_type`, `request_id`, and a diagnostic category. To use it, upload `glue/usage_stream.py` to the job's existing S3 script location before restarting. This diagnostic change retains `CODE_VERSION=1` and does not alter pricing or fact identity. The error handler deliberately omits raw signature-error bodies and signed headers.

| Diagnostic category | Check |
|---|---|
| Missing authentication token | Exact deployed stage, POST method, full resource path, and IAM signing. An existing API can return this error for a nonexistent route. |
| Access denied | Actual Glue execution role's resource ARN, stage capitalization, API resource policy, permissions boundaries, and organization denies. |
| Signature rejected or expired | Signing Region, `execute-api` service, current temporary role credentials, and clock. |
| Generic Forbidden | API key requirement, endpoint configuration, resource policy, or WAF. Use the request ID to correlate any configured API Gateway logs. |

The API Gateway console method test validates the Lambda integration using administrative test invocation. A successful method test does not establish that the Glue role can invoke the deployed URL. The producer's success confirms Kinesis acceptance, which is a separate step.

After a role-policy or API-deployment correction that leaves job arguments unchanged, restart Glue with the **same generation and checkpoint**. Already-published records remain available within Kinesis retention; rerunning the producer is unnecessary. If the correction changes `--CONTEXT_URL` or `--REGION`, the saved generation identity will reject the changed configuration. Allocate a fresh generation such as `live-002` for the corrected query, preserving the old checkpoint and staged files. This starts from the oldest retained stream records; existing fact keys remain protected from duplicates. Recover older missing records from the raw archive as described in section 17.

AWS documents the [API execution resource ARN format](https://docs.aws.amazon.com/apigateway/latest/developerguide/api-gateway-control-access-using-iam-policies-to-invoke-api.html) and the [different gateway errors that return HTTP 403](https://docs.aws.amazon.com/apigateway/latest/developerguide/supported-gateway-response-types.html).

## 21. Local checks and deployment acceptance

The Python Lambda files use the runtime-provided boto3 package. Glue needs the additional pinned dependencies from its configuration file. To run the offline checks in a local Python environment with boto3 installed:

```bash
python3 meterguard/tests/check_local.py
```

Run that command from the directory containing `meterguard`; from inside the project use `python3 tests/check_local.py`. The checks mock AWS clients and do not require credentials, Spark, or deployed resources. They exercise pricing precision, validation, version boundaries, partial retries, workflow conflicts, acknowledgement behavior, and configuration/schema alignment. They do not validate real Glue runtime behavior, IAM authorization, Redshift SQL execution, or service integration.

If boto3 is absent locally, create an isolated environment from inside `meterguard` before running the checks:

```bash
python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install boto3==1.40.0 botocore==1.40.0 s3transfer==0.13.1
python3 tests/check_local.py
```

Use Python 3.9 or later. On Windows, activate the environment with `.venv\Scripts\Activate.ps1` in PowerShell instead of `source`. This environment is only for local checks; AWS resources are still configured using the preceding console instructions.

Record the following deployment evidence before accepting the system:

| Acceptance check | Required observation |
|---|---|
| Initial usage | 100 facts, 2,000 billable pages, INR 800. |
| Sustained usage | Successful 900-second workflow, 180,000 facts across four named customers; per-customer totals match section 15. |
| Sustained retry | Same input and generator version reproduce the same operations; no additional fact keys or charge totals. |
| Duplicate input | No change to facts or totals. |
| New rate | 200 cumulative facts, 4,000 pages, INR 1,400; historical rate retained. |
| Missing reference | Quarantine record and reconciled reject count; no incorrect charge. |
| Commit-before-checkpoint crash | Recovery finishes at 210 facts / INR 1,480; no duplicate ledger or fact keys. |
| API outage | Failed batch, independent archive progress, successful restart after repair. |
| Historical replay | Existing facts/totals/outbox count unchanged; replay ledger has `publish_alerts=false`. |
| Alert processing | One logical case identity, observable notification, acknowledgement/escalation transition. |
| Notification failure | Pending work retained; logs/alarms identify failure and recovery succeeds. |

## 22. Stop resources and clean up

For a temporary pause, stop the producer workflow and wait for any active segment to finish, let Glue drain, stop the live Glue run, and disable the dispatcher schedule. Keep reference data, checkpoints, manifests, facts, and the independent archive for recovery. Kinesis retention continues to advance while Glue is stopped. Kinesis and other provisioned services can continue incurring charges; stopping a consumer does not delete its infrastructure.

For full removal, first export any records that must be retained and record that this ends the supported recovery period. Then use the consoles in this order:

1. Stop producer workflow executions and wait for any active producer Lambda segment to finish. Stop both Glue jobs. Disable `mg-dispatch-outbox` and the SQS event source mapping on `mg-starter`.
2. Allow or explicitly stop active workflow executions after reviewing their cases. Wait for Firehose delivery of remaining usage if the archive must be preserved.
3. Delete the Firehose stream, then the Kinesis stream. Delete the Glue jobs.
4. Delete the event rules, both state machines, Lambda functions (including the producer's published versions), and API Gateway REST API.
5. Delete the SQS queues and SNS subscriptions/topic after confirming pending alerts no longer need delivery.
6. Delete the Redshift workgroup and namespace, choosing a final snapshot only if needed. Retained snapshots continue to incur storage charges.
7. Delete the DynamoDB tables after any required backup/export. Review retained backups separately.
8. Empty the S3 bucket, including **all object versions and delete markers**, then delete it. This permanently removes raw replay data and checkpoints; do it only after the retention decision above.
9. Delete the project CloudWatch alarms/log groups and the ten execution roles, plus any optional EventBridge target role the console created. Remove any project-specific networking created solely for the workgroup after checking dependencies.
10. Review Billing/Cost Explorer for retained snapshots, backups, logs, and other project resources.

## Reference documentation

- [Standard Lambda timeout](https://docs.aws.amazon.com/lambda/latest/dg/configuration-timeout.html), [Step Functions Lambda integration](https://docs.aws.amazon.com/step-functions/latest/dg/connect-lambda.html), and [Kinesis throughput quotas](https://docs.aws.amazon.com/streams/latest/dev/service-sizes-and-limits.html).
- [Glue Kinesis connection options](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-connect-kinesis-home.html) and [Glue forEachBatch](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-api-crawler-pyspark-extensions-glue-context.html#aws-glue-api-crawler-pyspark-extensions-glue-context-forEachBatch).
- [Redshift stored-procedure transactions](https://docs.aws.amazon.com/redshift/latest/dg/stored-procedure-transaction-management.html) and [Data API authentication](https://docs.aws.amazon.com/redshift/latest/mgmt/data-api-access.html).
- [Step Functions StartExecution idempotency](https://docs.aws.amazon.com/step-functions/latest/apireference/API_StartExecution.html) and [execution redrive](https://docs.aws.amazon.com/step-functions/latest/dg/redrive-executions.html).
- [Lambda SQS partial batch responses](https://docs.aws.amazon.com/lambda/latest/dg/services-sqs-errorhandling.html) and [Firehose S3 prefixes](https://docs.aws.amazon.com/firehose/latest/dev/s3-prefixes.html).
