import psycopg2
import pandas as pd
import json
import time
from datetime import datetime
import os
from dotenv import load_dotenv
from pathlib import Path

# ---------------- PATHS & ENV ----------------
BASE_DIR = Path(__file__).resolve().parents[2]  # ETL_pipeline root
load_dotenv(dotenv_path=BASE_DIR / ".env")

DATA_PATH = BASE_DIR / "data/raw"
SUMMARY_PATH = BASE_DIR / "data/staging/ingestion_summary.json"

# ---------------- CONNECT ----------------
conn = psycopg2.connect(
    host=os.getenv("DB_HOST"),
    dbname=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    port=os.getenv("DB_PORT")
)

tables = ["customers", "products", "transactions", "transaction_items"]

summary = {"ingestion_timestamp": datetime.now().isoformat(), "tables_loaded": {}}
start = time.time()

try:
    with conn:
        with conn.cursor() as cur:
            for table in tables:
                print(f"🔄 Loading staging.{table}")

                # Get existing columns in staging table
                cur.execute(f"""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_schema='staging' AND table_name='{table}'
                """)
                existing_cols = [r[0] for r in cur.fetchall()]

                # Read CSV
                df = pd.read_csv(DATA_PATH / f"{table}.csv")

                # Keep only columns that exist in staging table
                df = df[[col for col in df.columns if col in existing_cols]]

                # Truncate table
                cur.execute(f"TRUNCATE staging.{table}")

                # Insert into staging
                for _, row in df.iterrows():
                    cols = ', '.join(df.columns)
                    placeholders = ', '.join(['%s'] * len(df.columns))
                    sql = f"INSERT INTO staging.{table} ({cols}) VALUES ({placeholders})"
                    cur.execute(sql, tuple(row[col] for col in df.columns))

                summary["tables_loaded"][f"staging.{table}"] = {
                    "rows_loaded": len(df),
                    "status": "success"
                }
                print(f"✅ Loaded {len(df)} rows into staging.{table}")

except Exception as e:
    conn.rollback()
    print(f"❌ Ingestion failed for {table}: {e}")
    summary["tables_loaded"][f"staging.{table}"] = {
        "rows_loaded": 0,
        "status": f"failed: {e}"
    }

finally:
    conn.close()

summary["total_execution_time_seconds"] = round(time.time() - start, 2)

# Save ingestion summary
Path(SUMMARY_PATH).parent.mkdir(parents=True, exist_ok=True)
with open(SUMMARY_PATH, "w") as f:
    json.dump(summary, f, indent=4)

print("📄 Ingestion summary written")
