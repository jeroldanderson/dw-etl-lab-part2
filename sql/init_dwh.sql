CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS dw;

CREATE TABLE IF NOT EXISTS staging.stg_sales (
    order_id TEXT,
    order_date DATE,
    store_id TEXT,
    store_name TEXT,
    barangay TEXT,
    city TEXT,
    product_id TEXT,
    product_name TEXT,
    category TEXT,
    quantity INTEGER,
    unit_price NUMERIC(10,2),
    line_total NUMERIC(12,2),
    batch_id TEXT
);

CREATE TABLE IF NOT EXISTS dw.dim_date (
    date_key INTEGER PRIMARY KEY,
    full_date DATE NOT NULL UNIQUE,
    day_of_week TEXT,
    is_weekend BOOLEAN,
    month INTEGER,
    month_name TEXT,
    quarter INTEGER,
    year INTEGER
);

CREATE TABLE IF NOT EXISTS dw.dim_store (
    store_key SERIAL PRIMARY KEY,
    store_id TEXT NOT NULL,  -- removed the UNIQUE constraint because we allow multiple versions of the same store_id (Type 2 SCD)
    store_name TEXT,
    barangay TEXT,
    city TEXT,

    valid_from DATE NOT NULL,
    valid_to DATE,
    is_current BOOLEAN NOT NULL DEFAULT TRUE,

    -- ensures that valid_to is either NULL or later than valid_from
    CONSTRAINT check_store_validity
        CHECK (valid_to IS NULL OR valid_to > valid_from),

    -- ensures that if is_current is TRUE, valid_to must be NULL, and if is_current is FALSE, valid_to must not be NULL 
    CONSTRAINT check_current_store
        CHECK (
            (is_current = TRUE AND valid_to IS NULL)
            OR
            (is_current = FALSE AND valid_to IS NOT NULL)
        ),

    -- makes sure that there are no overlapping validity periods for the same store_id
    CONSTRAINT unique_store_version
        UNIQUE (store_id, valid_from)
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_current_store
ON dw.dim_store (store_id)
WHERE is_current = TRUE;

CREATE TABLE IF NOT EXISTS dw.dim_product (
    product_key SERIAL PRIMARY KEY,
    product_id TEXT NOT NULL UNIQUE,
    product_name TEXT,
    category TEXT
);

CREATE TABLE IF NOT EXISTS dw.fact_sales (
    sales_key BIGSERIAL PRIMARY KEY,
    order_id TEXT NOT NULL,
    date_key INTEGER NOT NULL REFERENCES dw.dim_date(date_key),
    store_key INTEGER NOT NULL REFERENCES dw.dim_store(store_key),
    product_key INTEGER NOT NULL REFERENCES dw.dim_product(product_key),
    quantity INTEGER NOT NULL,
    unit_price NUMERIC(10,2) NOT NULL,
    line_total NUMERIC(12,2) NOT NULL,
    batch_id TEXT,
    UNIQUE (order_id, product_key)
);
