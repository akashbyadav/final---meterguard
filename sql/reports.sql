-- Run each query separately in Query Editor v2.
-- Select one processing version to avoid combining original/recalculated facts.

-- Monthly totals: compare these with the expected counts in the runbook.
SELECT *
FROM mg.tenant_usage
WHERE processing_version = 'v1'
ORDER BY billing_period, tenant_id;

-- Daily totals use the operation's event date, regardless of delivery time.
SELECT *
FROM mg.daily_usage
WHERE processing_version = 'v1'
ORDER BY usage_date, tenant_id;

-- The supplied sustained input creates 45,000 operations per named customer.
-- This filter isolates its run from the small customer-017 acceptance inputs.
-- For another run, replace the event-ID prefix with its exact run identity.
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

-- Fact-key integrity: this query must return ZERO rows.
SELECT
    tenant_id,
    event_id,
    processing_version,
    COUNT(*) AS copies
FROM mg.usage_fact
GROUP BY tenant_id, event_id, processing_version
HAVING COUNT(*) <> 1;

-- Ledger-key integrity: this query must also return ZERO rows.
SELECT
    generation,
    batch_id,
    COUNT(*) AS copies
FROM mg.batch_commits
GROUP BY generation, batch_id
HAVING COUNT(*) <> 1;

-- input_count counts deliveries, including duplicates. inserted_count counts
-- new operations. Each batch must reconcile input = valid + rejected.
SELECT
    generation,
    batch_id,
    input_count,
    valid_count,
    rejected_count,
    inserted_count,
    publish_alerts,
    committed_at
FROM mg.batch_commits
ORDER BY committed_at DESC;

-- sent_at confirms SQS acceptance, not completion of the workflow or email.
SELECT
    case_id,
    tenant_id,
    billing_period,
    estimated_total,
    budget_amount,
    created_at,
    sent_at
FROM mg.alert_outbox
ORDER BY created_at DESC;

-- Check pending warehouse intent separately from SQS queue age. A dispatcher
-- failure before publication can leave pending rows even when SQS is empty.
SELECT
    COUNT(*) AS pending_messages,
    MAX(DATEDIFF(second, created_at, GETDATE())) AS oldest_pending_seconds
FROM mg.alert_outbox
WHERE sent_at IS NULL;

-- Save this result before and after recovery/replay. Existing usage should
-- retain both its unique-operation count and its charge total.
SELECT
    processing_version,
    COUNT(*) AS operations,
    SUM(estimated_charge) AS estimated_total
FROM mg.usage_fact
GROUP BY processing_version
ORDER BY processing_version;
