from __future__ import annotations

import shutil
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from airflow.sdk import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

LANDING = Path("/opt/airflow/data/landing")
RAW = Path("/opt/airflow/datalake/raw/sales")
STAGED = Path("/opt/airflow/datalake/staged/sales")
DWH_CONN_ID = "dwh_postgres"

STG_COLUMNS = [
    "order_id", "order_date", "store_id", "store_name", "barangay", "city",
    "product_id", "product_name", "category", "quantity", "unit_price",
    "line_total", "batch_id",
]

# Runs once per batch, after that batch is in staging.stg_sales.
LOAD_DW_SQL = """
INSERT INTO dw.dim_date (date_key, full_date, day_of_week, is_weekend,
    month, month_name, quarter, year)
SELECT DISTINCT
    TO_CHAR(order_date, 'YYYYMMDD')::INT,
    order_date,
    TRIM(TO_CHAR(order_date, 'Day')),
    EXTRACT(ISODOW FROM order_date) IN (6, 7),
    EXTRACT(MONTH FROM order_date)::INT,
    TRIM(TO_CHAR(order_date, 'Month')),
    EXTRACT(QUARTER FROM order_date)::INT,
    EXTRACT(YEAR FROM order_date)::INT
FROM staging.stg_sales
ON CONFLICT (date_key) DO NOTHING;

-- dim_store: SCD Type 2 (same steps as sql/scd2_store_load.sql)

-- 1. Latest store details in this batch.
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

-- 2. New stores, and stores whose details differ from their current version.
--    valid_from = first order date in this batch that shows these details.
CREATE TEMP TABLE scd2_changes AS
SELECT
    incoming.store_id,
    incoming.store_name,
    incoming.barangay,
    incoming.city,
    (
        SELECT MIN(s.order_date)
        FROM staging.stg_sales s
        WHERE s.store_id = incoming.store_id
          AND (s.store_name, s.barangay, s.city)
              IS NOT DISTINCT FROM
              (incoming.store_name, incoming.barangay, incoming.city)
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

-- 3. Expire the current version, only if the change is newer than it.
UPDATE dw.dim_store old
SET
    valid_to = changes.valid_from,
    is_current = FALSE
FROM scd2_changes changes
WHERE old.store_id = changes.store_id
  AND old.is_current = TRUE
  AND changes.valid_from > old.valid_from;

-- 4. Insert the new current version. Skipped while a current version still
--    exists (for example when an older batch is re-run), so re-runs never
--    add duplicate versions.
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

INSERT INTO dw.dim_product (product_id, product_name, category)
SELECT DISTINCT ON (product_id) product_id, product_name, category
FROM staging.stg_sales
ORDER BY product_id
ON CONFLICT (product_id) DO UPDATE
    SET product_name = EXCLUDED.product_name,
        category = EXCLUDED.category;

-- Each sale joins the store version that was valid on its order date.
INSERT INTO dw.fact_sales (order_id, date_key, store_key, product_key,
    quantity, unit_price, line_total, batch_id)
SELECT s.order_id,
    TO_CHAR(s.order_date, 'YYYYMMDD')::INT,
    ds.store_key,
    dp.product_key,
    s.quantity, s.unit_price, s.line_total, s.batch_id
FROM staging.stg_sales s
JOIN dw.dim_store ds
    ON ds.store_id = s.store_id
    AND s.order_date >= ds.valid_from
    AND (
        s.order_date < ds.valid_to
        OR ds.valid_to IS NULL
    )
JOIN dw.dim_product dp ON dp.product_id = s.product_id
ON CONFLICT (order_id, product_key) DO UPDATE
    SET date_key = EXCLUDED.date_key,
        store_key = EXCLUDED.store_key,
        quantity = EXCLUDED.quantity,
        unit_price = EXCLUDED.unit_price,
        line_total = EXCLUDED.line_total,
        batch_id = EXCLUDED.batch_id;
"""


def clean_sales(df: pd.DataFrame) -> pd.DataFrame:
    """Part 1 cleaning rules, applied to one batch (one landing file)."""
    df = df.apply(lambda col: col.str.strip())
    df["category"] = df["category"].str.title()
    df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce").dt.date
    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce")
    df["unit_price"] = pd.to_numeric(df["unit_price"], errors="coerce")
    required = ["order_id", "order_date", "store_id", "product_id",
                "quantity", "unit_price"]
    df = df.dropna(subset=required)
    df = df[(df["quantity"] > 0) & (df["unit_price"] >= 0)]
    df = df.drop_duplicates(subset=["order_id", "product_id"], keep="first")
    df["quantity"] = df["quantity"].astype(int)
    df["line_total"] = (df["quantity"] * df["unit_price"]).round(2)
    return df


@dag(
    dag_id="sarisari_sales_etl",
    schedule="@daily",
    start_date=datetime(2026, 10, 1),
    catchup=False,
    default_args={"retries": 1},
    tags=["cselec1c", "etl"],
)
def sarisari_sales_etl():

    @task
    def extract() -> dict:
        """E: copy landing files, untouched, into a new batch folder in the raw zone."""
        files = sorted(LANDING.glob("sales_*.csv"))
        if not files:
            raise FileNotFoundError(f"No sales_*.csv files found in {LANDING}")
        batch_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
        target = RAW / f"ingest_date={batch_id[:8]}" / f"batch={batch_id}"
        target.mkdir(parents=True, exist_ok=True)
        copied = []
        for f in files:
            shutil.copy2(f, target / f.name)
            copied.append(str(target / f.name))
        print(f"Copied {len(copied)} file(s) to {target}")
        return {"batch_id": batch_id, "files": copied}

    @task
    def transform(raw: dict) -> dict:
        """T: clean each raw file on its own and write one Parquet per file.

        Files are not combined. Each file is one batch, and the batches are
        returned oldest first (by earliest order_date) so load() applies
        store changes in the order they happened.
        """
        batches = []
        rows_in = rows_out = 0
        for path in raw["files"]:
            name = Path(path).name
            df = pd.read_csv(path, dtype=str)
            n_in = len(df)
            df = clean_sales(df)
            df["batch_id"] = raw["batch_id"]
            rows_in += n_in
            rows_out += len(df)
            print(f"{name}: rows in {n_in}, kept {len(df)}, rejected {n_in - len(df)}")
            if df.empty:
                continue
            out = STAGED / f"batch={raw['batch_id']}" / f"{Path(path).stem}.parquet"
            out.parent.mkdir(parents=True, exist_ok=True)
            df[STG_COLUMNS].to_parquet(out, index=False)
            batches.append({"file": name, "path": str(out),
                            "first_date": str(df["order_date"].min()),
                            "rows": len(df)})
        batches.sort(key=lambda b: (b["first_date"], b["file"]))
        print("Load order: " + " -> ".join(b["file"] for b in batches))
        print(f"Total rows in: {rows_in}, kept: {rows_out}, rejected: {rows_in - rows_out}")
        return {"batch_id": raw["batch_id"], "batches": batches,
                "rows_in": rows_in, "rows_out": rows_out}

    @task
    def load(staged: dict) -> dict:
        """L: for each batch, oldest first: Parquet -> staging -> dimensions and fact."""
        hook = PostgresHook(postgres_conn_id=DWH_CONN_ID)
        rows_loaded = 0
        for batch in staged["batches"]:
            df = pd.read_parquet(batch["path"])
            hook.run("TRUNCATE staging.stg_sales;")
            rows = df[STG_COLUMNS].astype(object).itertuples(index=False, name=None)
            hook.insert_rows(table="staging.stg_sales", rows=rows,
                             target_fields=STG_COLUMNS, commit_every=1000)
            hook.run(LOAD_DW_SQL)
            rows_loaded += len(df)
            print(f"Loaded {batch['file']}: {len(df)} rows "
                  f"(first order date {batch['first_date']})")
        fact_rows = hook.get_first("SELECT COUNT(*) FROM dw.fact_sales")[0]
        versions = hook.get_first("SELECT COUNT(*) FROM dw.dim_store")[0]
        print(f"Loaded {rows_loaded} rows in {len(staged['batches'])} batch(es); "
              f"fact_sales has {fact_rows} rows, dim_store has {versions} store versions")
        return {"batch_id": staged["batch_id"], "rows_loaded": rows_loaded}

    @task
    def quality_check(loaded: dict) -> None:
        """Fail the run loudly if the warehouse breaks a business rule."""
        hook = PostgresHook(postgres_conn_id=DWH_CONN_ID)
        checks = {
            "fact table is not empty":
                "SELECT COUNT(*) > 0 FROM dw.fact_sales",
            "every loaded row reached the fact table":
                "SELECT COUNT(*) = %(n)s FROM dw.fact_sales WHERE batch_id = %(b)s",
            "no non-positive quantities":
                "SELECT COUNT(*) = 0 FROM dw.fact_sales WHERE quantity <= 0",
            "line_total equals quantity x unit_price":
                "SELECT COUNT(*) = 0 FROM dw.fact_sales "
                "WHERE line_total <> ROUND(quantity * unit_price, 2)",
            "no unit price above 10000":
                "SELECT COUNT(*) = 0 FROM dw.fact_sales WHERE unit_price > 10000",
            "each store has exactly one current version":
                "SELECT COUNT(*) = 0 FROM ("
                " SELECT store_id FROM dw.dim_store GROUP BY store_id"
                " HAVING COUNT(*) FILTER (WHERE is_current) <> 1) bad",
            "every fact points to the store version valid on its order date":
                "SELECT COUNT(*) = 0 FROM dw.fact_sales f "
                "JOIN dw.dim_store s ON s.store_key = f.store_key "
                "JOIN dw.dim_date d ON d.date_key = f.date_key "
                "WHERE d.full_date < s.valid_from "
                "OR (s.valid_to IS NOT NULL AND d.full_date >= s.valid_to)",
        }
        params = {"b": loaded["batch_id"], "n": loaded["rows_loaded"]}
        failed = [name for name, sql in checks.items()
                  if not hook.get_first(sql, parameters=params)[0]]
        if failed:
            raise ValueError(f"Data quality checks failed: {failed}")
        print(f"All {len(checks)} checks passed for batch {loaded['batch_id']}")

    quality_check(load(transform(extract())))

sarisari_sales_etl()