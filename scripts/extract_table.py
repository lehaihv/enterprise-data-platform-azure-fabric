import os
import sys

import pandas as pd
import psycopg
from dotenv import load_dotenv
from datetime import datetime, timezone

load_dotenv()

connection_string = os.environ["NEON_DATABASE_URL"]


if len(sys.argv) != 2:
    print("Usage:")
    print("python extract_table.py customer")
    sys.exit(1)


table_name = sys.argv[1]

allowed_tables = {
    "customer",
    "product",
    "sales_order",
    "order_item"
}


if table_name not in allowed_tables:
    raise ValueError(
        f"Table not allowed: {table_name}"
    )


query = f"""
SELECT *
FROM {table_name}
"""


with psycopg.connect(connection_string) as conn:

    df = pd.read_sql(
        query,
        conn
    )

df["ingestion_timestamp"] = datetime.now(timezone.utc)


os.makedirs(
    "data/bronze",
    exist_ok=True
)


output_path = (
    f"data/bronze/{table_name}.parquet"
)


df.to_parquet(
    output_path,
    index=False,
    coerce_timestamps="us",
    allow_truncated_timestamps=True
)


print(
    f"Extracted {len(df)} rows "
    f"from {table_name}"
)

print(
    f"Written to {output_path}"
)