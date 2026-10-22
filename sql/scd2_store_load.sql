-- Identify the most recent store information in staging.

CREATE TEMP TABLE scd2_incoming_stores AS
SELECT DISTINCT ON (s.store_id)
    s.store_id,
    s.store_name,
    s.barangay,
    s.city,
    s.order_date AS effective_date
FROM staging.stg_sales s
ORDER BY
    s.store_id,
    s.order_date DESC,
    s.order_id DESC;

-- Identify new stores and changed existing stores.

CREATE TEMP TABLE scd2_changes AS
SELECT
    incoming.store_id,
    incoming.store_name,
    incoming.barangay,
    incoming.city,
    CASE
        WHEN current_store.store_key IS NULL
        THEN (
            SELECT MIN(s.order_date)
            FROM staging.stg_sales s
            WHERE s.store_id = incoming.store_id
        )
        ELSE incoming.effective_date
    END AS valid_from
FROM scd2_incoming_stores incoming
LEFT JOIN dw.dim_store current_store
    ON current_store.store_id = incoming.store_id
    AND current_store.is_current = TRUE
WHERE
    current_store.store_key IS NULL
    OR (
        current_store.store_name,
        current_store.barangay,
        current_store.city
    ) IS DISTINCT FROM (
        incoming.store_name,
        incoming.barangay,
        incoming.city
    );

-- Expire the previous version.

UPDATE dw.dim_store old
SET
    valid_to = changes.valid_from,
    is_current = FALSE
FROM scd2_changes changes
WHERE old.store_id = changes.store_id
  AND old.is_current = TRUE
  AND changes.valid_from > old.valid_from;

-- Insert the new version.

INSERT INTO dw.dim_store (
    store_id,
    store_name,
    barangay,
    city,
    valid_from,
    valid_to,
    is_current
)
SELECT
    changes.store_id,
    changes.store_name,
    changes.barangay,
    changes.city,
    changes.valid_from,
    NULL,
    TRUE
FROM scd2_changes changes
WHERE NOT EXISTS (
    SELECT 1
    FROM dw.dim_store d
    WHERE d.store_id = changes.store_id
      AND d.is_current = TRUE
)
ON CONFLICT (store_id, valid_from) DO NOTHING;

DROP TABLE scd2_changes;
DROP TABLE scd2_incoming_stores;