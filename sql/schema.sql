-- Run in Query Editor v2 as the namespace administrator.
-- Redshift logical keys are enforced by commit_batch, not by PRIMARY KEY
-- declarations: those declarations would not enforce row uniqueness.
CREATE SCHEMA IF NOT EXISTS mg;

-- All application writers lock this table before reading facts or the ledger.
-- The table stays empty; its table-level lock is the shared coordination point.
CREATE TABLE IF NOT EXISTS mg.writer_lock (
    purpose VARCHAR(64)
);

-- One result per customer, operation, and processing version. Keep the column
-- order aligned with FACT_COLUMNS in glue/usage_stream.py.
CREATE TABLE IF NOT EXISTS mg.usage_fact (
    -- Business identity and when the operation occurred.
    tenant_id VARCHAR(64) NOT NULL,
    event_id VARCHAR(128) NOT NULL,
    processing_version VARCHAR(64) NOT NULL,
    request_id VARCHAR(128) NOT NULL,
    event_time TIMESTAMP NOT NULL,
    billing_period DATE NOT NULL,

    -- Usage measurements come from the source event.
    service VARCHAR(64) NOT NULL,
    pages_processed INTEGER NOT NULL,
    outcome VARCHAR(16) NOT NULL,
    contract_version VARCHAR(64) NOT NULL,

    -- Resolved reference values are retained on every fact for historical audit.
    rate_id VARCHAR(64) NOT NULL,
    currency VARCHAR(3) NOT NULL,
    rate_per_page DECIMAL(18,6) NOT NULL,
    estimated_charge DECIMAL(18,6) NOT NULL,
    budget_amount DECIMAL(18,6) NOT NULL,
    budget_policy_version VARCHAR(64) NOT NULL,

    -- Hashes distinguish a harmless repeated result from a conflicting one.
    source_hash VARCHAR(64) NOT NULL,
    result_hash VARCHAR(64) NOT NULL
) DISTSTYLE AUTO SORTKEY(billing_period, tenant_id);

COMMENT ON TABLE mg.usage_fact IS
    'One immutable result per (tenant_id, event_id, processing_version); amounts are estimates.';

-- A generation survives job restarts. batch_id is unique only inside that
-- generation; another replay or live generation can have its own batch zero.
CREATE TABLE IF NOT EXISTS mg.batch_commits (
    generation VARCHAR(64) NOT NULL,
    batch_id BIGINT NOT NULL,
    processing_version VARCHAR(64) NOT NULL,

    -- Input counts include repeated deliveries. inserted_count counts only
    -- genuinely new business keys, so it can be lower than valid_count.
    input_count BIGINT NOT NULL,
    valid_count BIGINT NOT NULL,
    rejected_count BIGINT NOT NULL,
    inserted_count BIGINT NOT NULL,

    manifest_uri VARCHAR(2048) NOT NULL,
    publish_alerts BOOLEAN NOT NULL,
    committed_at TIMESTAMP NOT NULL DEFAULT GETDATE()
) DISTSTYLE ALL;

COMMENT ON TABLE mg.batch_commits IS
    'One commit per (generation, batch_id); generation survives job restarts.';

-- Durable notification intent is committed alongside facts. The dispatcher
-- fills sent_at only after SQS accepts the corresponding message.
CREATE TABLE IF NOT EXISTS mg.alert_outbox (
    case_id VARCHAR(32) NOT NULL,
    tenant_id VARCHAR(64) NOT NULL,
    billing_period DATE NOT NULL,
    processing_version VARCHAR(64) NOT NULL,
    budget_policy_version VARCHAR(64) NOT NULL,
    currency VARCHAR(3) NOT NULL,
    estimated_total DECIMAL(28,6) NOT NULL,
    budget_amount DECIMAL(18,6) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT GETDATE(),
    sent_at TIMESTAMP
) DISTSTYLE ALL;

COMMENT ON TABLE mg.alert_outbox IS
    'Durable intent to notify; sent_at records SQS acceptance, not email delivery.';

-- Group by processing version so recalculated versions do not double-count
-- the same operations. Failed operations remain visible but contribute no pages
-- to billable usage and have zero estimated charge.
CREATE OR REPLACE VIEW mg.tenant_usage AS
SELECT
    tenant_id,
    billing_period,
    processing_version,
    currency,
    COUNT(*) AS operations,
    SUM(
        CASE
            WHEN outcome = 'SUCCESS' THEN pages_processed
            ELSE 0
        END
    ) AS billable_pages,
    SUM(estimated_charge) AS estimated_total
FROM mg.usage_fact
GROUP BY
    tenant_id,
    billing_period,
    processing_version,
    currency;

-- The daily view uses the event's UTC calendar date, not its arrival date.
CREATE OR REPLACE VIEW mg.daily_usage AS
SELECT
    tenant_id,
    CAST(event_time AS DATE) AS usage_date,
    processing_version,
    currency,
    COUNT(*) AS operations,
    SUM(
        CASE
            WHEN outcome = 'SUCCESS' THEN pages_processed
            ELSE 0
        END
    ) AS billable_pages,
    SUM(estimated_charge) AS estimated_total
FROM mg.usage_fact
GROUP BY
    tenant_id,
    CAST(event_time AS DATE),
    processing_version,
    currency;

-- IAM authentication maps the execution roles to these database users.
-- Run CREATE USER once; skip existing users when rerunning schema setup.
CREATE USER "IAMR:mg-glue-role" PASSWORD DISABLE;
CREATE USER "IAMR:mg-dispatcher-role" PASSWORD DISABLE;

GRANT USAGE ON SCHEMA mg
TO "IAMR:mg-glue-role", "IAMR:mg-dispatcher-role";

GRANT SELECT, UPDATE ON mg.alert_outbox
TO "IAMR:mg-dispatcher-role";

-- commit_batch.sql grants Glue EXECUTE on the procedure. Glue has no direct
-- fact-table write grant, so its writes must follow the commit contract.
