
-- finds most recent store details from staging

CREATE TEMP TABLE scd2_incoming_stores AS
SELECT DISTINCT ON (s.store_id)
    s.store_id,
    s.store_name,
    s.barangay,
    s.city
FROM staging.stg_sales s
ORDER BY
    s.store_id,
    s.order_date DESC,
    s.order_id DESC;


-- identify the new stores and changed existing stores. 
-- for changed existing stores, we will create a new version with the updated details and expire the previous version

CREATE TEMP TABLE scd2_changes AS
SELECT
    incoming.store_id,
    incoming.store_name,
    incoming.barangay,
    incoming.city,

    -- use the MIN() function to get the earliest order date for the store with the same details
    (
        SELECT MIN(s.order_date)
        FROM staging.stg_sales s
        WHERE s.store_id = incoming.store_id
          AND (
              s.store_name,
              s.barangay,
              s.city
          ) IS NOT DISTINCT FROM (
              incoming.store_name,
              incoming.barangay,
              incoming.city
          )
    ) AS valid_from

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


-- do not allow changes that would make invalid date ranges

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM scd2_changes changes
        JOIN dw.dim_store old
            ON old.store_id = changes.store_id
           AND old.is_current = TRUE
        WHERE changes.valid_from <= old.valid_from
    ) THEN
        RAISE EXCEPTION
            'SCD Type 2 change date must be later than current version start date';
    END IF;
END
$$;


-- make sure old version is expired before inserting the new version

UPDATE dw.dim_store old
SET
    valid_to = changes.valid_from,
    is_current = FALSE
FROM scd2_changes changes
WHERE old.store_id = changes.store_id
  AND old.is_current = TRUE
  AND changes.valid_from > old.valid_from;


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


-- drop the temp tables created earlier

DROP TABLE scd2_changes;
DROP TABLE scd2_incoming_stores;
