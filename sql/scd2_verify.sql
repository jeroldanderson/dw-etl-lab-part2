-- show all historical versions of each store

SELECT
    store_key,
    store_id,
    store_name,
    barangay,
    valid_from,
    valid_to,
    is_current
FROM dw.dim_store
ORDER BY store_id, valid_from;


-- verify that each store has one current version

SELECT
    store_id,
    COUNT(*) FILTER (WHERE is_current = TRUE)
        AS current_versions
FROM dw.dim_store
GROUP BY store_id
ORDER BY store_id;


-- show the historical sales and their associated store versions

SELECT
    f.order_id,
    d.full_date AS order_date,
    s.store_id,
    s.store_key,
    s.barangay,
    s.valid_from,
    s.valid_to,
    f.line_total
FROM dw.fact_sales f
JOIN dw.dim_store s
    ON f.store_key = s.store_key
JOIN dw.dim_date d
    ON f.date_key = d.date_key
ORDER BY s.store_id, d.full_date, f.order_id;


-- check that facts reference the correct version for their transaction dates.

SELECT COUNT(*) AS incorrectly_linked_facts
FROM dw.fact_sales f
JOIN dw.dim_store s
    ON f.store_key = s.store_key
JOIN dw.dim_date d
    ON f.date_key = d.date_key
WHERE NOT (
    d.full_date >= s.valid_from
    AND (
        d.full_date < s.valid_to
        OR s.valid_to IS NULL
    )
);


-- check for stores with multiple current versions.

SELECT
    store_id,
    COUNT(*) AS current_version_count
FROM dw.dim_store
WHERE is_current = TRUE
GROUP BY store_id
HAVING COUNT(*) > 1;