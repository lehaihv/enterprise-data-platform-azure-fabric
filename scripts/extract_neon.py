import os

import pandas as pd
import psycopg
from dotenv import load_dotenv
from datetime import datetime, timezone


load_dotenv()

connection_string = os.environ["NEON_DATABASE_URL"]

conn = psycopg.connect(connection_string)

query = """
SELECT
    customer_id,
    customer_name,
    email,
    state,
    postcode,
    created_date,
    modified_date
FROM customer
ORDER BY customer_id
"""

df = pd.read_sql(query, conn)

conn.close()

# Add ingestion timestamp
df["ingestion_timestamp"] = datetime.now(timezone.utc)

# Write Bronze
os.makedirs("data/bronze", exist_ok=True)

df.to_parquet(
    "data/bronze/customer.parquet",
    index=False
)

print(f"Extracted {len(df)} rows")
print("Bronze file created.")