-- Commit one completed Spark batch as one warehouse transaction.
-- Default atomic mode is essential: do not add NONATOMIC, COMMIT, ROLLBACK,
-- or TRUNCATE. The caller waits for this entire procedure to finish.
CREATE OR REPLACE PROCEDURE mg.commit_batch(
    p_generation VARCHAR(64),
    p_batch_id BIGINT,
    p_version VARCHAR(64),
    p_manifest VARCHAR(2048),
    p_copy_role VARCHAR(2048),
    p_input_count BIGINT,
    p_valid_count BIGINT,
    p_rejected_count BIGINT,
    p_publish BOOLEAN
)
AS $$
DECLARE
    found_count BIGINT;
    new_count BIGINT;
BEGIN
    -- 1. Serialize writers BEFORE reading the ledger or facts. Live processing
    --    and replay both take this lock and retain it until the call completes.
    --    A serialization error fails the transaction and remains safe to retry.
    LOCK TABLE mg.writer_lock;

    SELECT COUNT(*)
    INTO found_count
    FROM mg.batch_commits
    WHERE generation = p_generation
        AND batch_id = p_batch_id;

    IF found_count > 0 THEN
        -- An existing batch is a safe retry only if it references the same
        -- completed output and processing contract. A reused ID with changed
        -- inputs must fail instead of silently skipping those inputs.
        SELECT COUNT(*)
        INTO found_count
        FROM mg.batch_commits
        WHERE generation = p_generation
            AND batch_id = p_batch_id
            AND processing_version = p_version
            AND manifest_uri = p_manifest
            AND input_count = p_input_count
            AND valid_count = p_valid_count
            AND rejected_count = p_rejected_count
            AND publish_alerts = p_publish;

        IF found_count <> 1 THEN
            RAISE EXCEPTION 'Batch identity was reused with different input or configuration';
        END IF;

        RETURN;
    END IF;

    -- 2. Validate the batch accounting before loading files. Rejected records
    --    are already stored in quarantine; they do not enter the fact table.
    IF p_input_count <> p_valid_count + p_rejected_count
        OR p_valid_count < 0
        OR p_rejected_count < 0
    THEN
        RAISE EXCEPTION 'Batch counts do not reconcile';
    END IF;

    -- Temporary staging is private to this Data API session. CREATE fails on
    -- an unexpected existing name instead of reusing unknown intermediate data.
    CREATE TEMP TABLE mg_stage (LIKE mg.usage_fact);

    IF p_valid_count > 0 THEN
        -- 3. Load only the completed files named by the manifest. quote_literal
        --    protects dynamic SQL arguments; the COPY role can read staged data.
        --    A zero-valid-row batch skips COPY but still records its rejects.
        EXECUTE
            'COPY mg_stage FROM ' || quote_literal(p_manifest)
            || ' IAM_ROLE ' || quote_literal(p_copy_role)
            || ' MANIFEST FORMAT AS JSON ''auto'''
            || ' TIMEFORMAT ''auto'' DATEFORMAT ''auto''';
    END IF;

    SELECT COUNT(*)
    INTO found_count
    FROM mg_stage;

    IF found_count <> p_valid_count THEN
        RAISE EXCEPTION 'Loaded row count differs from the completed manifest';
    END IF;

    SELECT COUNT(*)
    INTO found_count
    FROM mg_stage
    WHERE processing_version <> p_version;

    IF found_count > 0 THEN
        RAISE EXCEPTION 'Manifest contains another processing version';
    END IF;

    -- 4. Exact repeated results are valid duplicate deliveries. Different
    --    results for the same business key indicate a payload/pricing conflict.
    --    Check both within this batch and against previously committed facts.
    SELECT COUNT(*)
    INTO found_count
    FROM (
        SELECT
            tenant_id,
            event_id,
            processing_version
        FROM mg_stage
        GROUP BY
            tenant_id,
            event_id,
            processing_version
        HAVING COUNT(DISTINCT result_hash) > 1
    ) conflicts;

    IF found_count > 0 THEN
        RAISE EXCEPTION 'Conflicting duplicate events inside this batch';
    END IF;

    SELECT COUNT(*)
    INTO found_count
    FROM mg_stage s
    JOIN mg.usage_fact f
        ON s.tenant_id = f.tenant_id
        AND s.event_id = f.event_id
        AND s.processing_version = f.processing_version
    WHERE s.result_hash <> f.result_hash;

    IF found_count > 0 THEN
        RAISE EXCEPTION 'Existing event has a different payload or pricing snapshot';
    END IF;

    -- 5. Each customer's billing month has one currency and budget policy.
    --    A price can change during that month, but dividing one budget across
    --    different policies would produce misleading threshold calculations.
    SELECT COUNT(*)
    INTO found_count
    FROM (
        SELECT
            tenant_id,
            billing_period,
            processing_version
        FROM (
            SELECT
                tenant_id,
                billing_period,
                processing_version,
                budget_policy_version,
                currency,
                budget_amount
            FROM mg_stage

            UNION ALL

            -- Include existing facts only for customer/month/version groups
            -- touched by this batch; unrelated customers need no revalidation.
            SELECT
                f.tenant_id,
                f.billing_period,
                f.processing_version,
                f.budget_policy_version,
                f.currency,
                f.budget_amount
            FROM mg.usage_fact f
            WHERE EXISTS (
                SELECT 1
                FROM mg_stage s
                WHERE s.tenant_id = f.tenant_id
                    AND s.billing_period = f.billing_period
                    AND s.processing_version = f.processing_version
            )
        ) policies
        GROUP BY
            tenant_id,
            billing_period,
            processing_version
        HAVING COUNT(DISTINCT currency) > 1
            OR COUNT(DISTINCT budget_amount) > 1
            OR COUNT(DISTINCT budget_policy_version) > 1
    ) conflicting_policies;

    IF found_count > 0 THEN
        RAISE EXCEPTION 'Currency or budget policy changed within one tenant billing period';
    END IF;

    -- 6. Keep one copy of each equal result. The earlier conflict checks make
    --    the row-number choice safe: there are no competing prices to choose.
    CREATE TEMP TABLE mg_unique AS
    SELECT *
    FROM mg_stage AS staged
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY tenant_id, event_id, processing_version
        ORDER BY result_hash
    ) = 1;

    SELECT COUNT(*)
    INTO new_count
    FROM mg_unique s
    WHERE NOT EXISTS (
        SELECT 1
        FROM mg.usage_fact f
        WHERE f.tenant_id = s.tenant_id
            AND f.event_id = s.event_id
            AND f.processing_version = s.processing_version
    );

    -- 7. Insert only new business keys. writer_lock is still held, so another
    --    writer cannot race the NOT EXISTS check. Existing equal facts remain
    --    unchanged, even when replay uses different batch boundaries.
    INSERT INTO mg.usage_fact (
        tenant_id,
        event_id,
        processing_version,
        request_id,
        event_time,
        billing_period,
        service,
        pages_processed,
        outcome,
        contract_version,
        rate_id,
        currency,
        rate_per_page,
        estimated_charge,
        budget_amount,
        budget_policy_version,
        source_hash,
        result_hash
    )
    SELECT
        s.tenant_id,
        s.event_id,
        s.processing_version,
        s.request_id,
        s.event_time,
        s.billing_period,
        s.service,
        s.pages_processed,
        s.outcome,
        s.contract_version,
        s.rate_id,
        s.currency,
        s.rate_per_page,
        s.estimated_charge,
        s.budget_amount,
        s.budget_policy_version,
        s.source_hash,
        s.result_hash
    FROM mg_unique s
    WHERE NOT EXISTS (
        SELECT 1
        FROM mg.usage_fact f
        WHERE f.tenant_id = s.tenant_id
            AND f.event_id = s.event_id
            AND f.processing_version = s.processing_version
    );

    IF p_publish THEN
        -- 8. Evaluate budget thresholds from the committed-result set within
        --    this SAME transaction. No separate running counter is incremented.
        --    Replay passes false and bypasses all new notification intent.
        INSERT INTO mg.alert_outbox (
            case_id,
            tenant_id,
            billing_period,
            processing_version,
            budget_policy_version,
            currency,
            estimated_total,
            budget_amount
        )
        SELECT
            candidate.case_id,
            candidate.tenant_id,
            candidate.billing_period,
            candidate.processing_version,
            candidate.budget_policy_version,
            candidate.currency,
            candidate.estimated_total,
            candidate.budget_amount
        FROM (
            -- The case ID depends on business identity, not batch ID or time
            -- of delivery. Later threshold crossings reuse the same logical case.
            SELECT
                MD5(
                    f.tenant_id || '|'
                    || f.billing_period::VARCHAR || '|'
                    || f.processing_version || '|'
                    || f.budget_policy_version || '|'
                    || f.currency
                ) AS case_id,
                f.tenant_id,
                f.billing_period,
                f.processing_version,
                f.budget_policy_version,
                f.currency,
                SUM(f.estimated_charge) AS estimated_total,
                MAX(f.budget_amount) AS budget_amount
            FROM mg.usage_fact f
            WHERE EXISTS (
                SELECT 1
                FROM mg_stage s
                WHERE s.tenant_id = f.tenant_id
                    AND s.billing_period = f.billing_period
                    AND s.processing_version = f.processing_version
            )
            GROUP BY
                f.tenant_id,
                f.billing_period,
                f.processing_version,
                f.budget_policy_version,
                f.currency
            HAVING SUM(f.estimated_charge) >= MAX(f.budget_amount)
        ) candidate
        WHERE NOT EXISTS (
            SELECT 1
            FROM mg.alert_outbox o
            WHERE o.case_id = candidate.case_id
        );
    END IF;

    -- 9. Record completion only after facts and alert intent have succeeded.
    --    Any earlier exception rolls back these writes together.
    INSERT INTO mg.batch_commits (
        generation,
        batch_id,
        processing_version,
        input_count,
        valid_count,
        rejected_count,
        inserted_count,
        manifest_uri,
        publish_alerts
    )
    VALUES (
        p_generation,
        p_batch_id,
        p_version,
        p_input_count,
        p_valid_count,
        p_rejected_count,
        new_count,
        p_manifest,
        p_publish
    );

    -- Successful return commits this call. Only then can Glue return from its
    -- callback and allow Spark to complete the checkpoint for this batch.
END;
$$ LANGUAGE plpgsql SECURITY DEFINER SET search_path TO mg;

-- Application writers can execute this procedure without direct table-write
-- privileges. Keep PUBLIC from invoking it through an unintended identity.
REVOKE ALL ON PROCEDURE mg.commit_batch(
    VARCHAR,
    BIGINT,
    VARCHAR,
    VARCHAR,
    VARCHAR,
    BIGINT,
    BIGINT,
    BIGINT,
    BOOLEAN
) FROM PUBLIC;

GRANT EXECUTE ON PROCEDURE mg.commit_batch(
    VARCHAR,
    BIGINT,
    VARCHAR,
    VARCHAR,
    VARCHAR,
    BIGINT,
    BIGINT,
    BIGINT,
    BOOLEAN
) TO "IAMR:mg-glue-role";
