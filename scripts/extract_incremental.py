import os
import sys
from datetime import datetime, timezone

import pandas as pd
import psycopg
from dotenv import load_dotenv


load_dotenv()

connection_string = os.environ["NEON_DATABASE_URL"]


if len(sys.argv) != 2:
    print("Usage:")
    print("python extract_incremental.py customer")
    sys.exit(1)


table_name = sys.argv[1]

allowed_tables = {
    "customer",
    "product",
    "sales_order",
    "order_item"
}

if table_name not in allowed_tables:
    raise ValueError(f"Table not allowed: {table_name}")


pipeline_name = f"{table_name}_incremental"


with psycopg.connect(connection_string) as conn:

    # Get the previous watermark
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT last_successful_modified_date
            FROM pipeline_control
            WHERE pipeline_name = %s
            """,
            (pipeline_name,)
        )

        result = cur.fetchone()

        if result is None:
            raise ValueError(
                f"No control record found for {pipeline_name}"
            )

        watermark = result[0]


    # Extract only changed records
    query = f"""
        SELECT *
        FROM {table_name}
        WHERE modified_date > %s
        ORDER BY modified_date
    """

    df = pd.read_sql(
        query,
        conn,
        params=(watermark,)
    )


# Add Bronze metadata
df["ingestion_timestamp"] = datetime.now(timezone.utc)


# Write Bronze
os.makedirs("data/bronze", exist_ok=True)

output_path = f"data/bronze/{table_name}_incremental.parquet"

df.to_parquet(
    output_path,
    index=False,
    coerce_timestamps="us",
    allow_truncated_timestamps=True
)


print(f"Previous watermark: {watermark}")
print(f"Rows extracted: {len(df)}")
print(f"Written to: {output_path}")