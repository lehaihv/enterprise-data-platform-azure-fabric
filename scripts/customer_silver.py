from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    trim,
    upper,
    lower,
    coalesce,
    lit
)


spark = (
    SparkSession.builder
    .appName("CustomerSilver")
    .master("local[*]")
    .getOrCreate()
)


# Read Bronze
bronze_df = spark.read.parquet(
    "data/bronze/customer.parquet"
)


print("=== BRONZE ===")
bronze_df.show()


# Clean customer name
silver_df = bronze_df.withColumn(
    "customer_name",
    trim(col("customer_name"))
)


# Clean email
silver_df = silver_df.withColumn(
    "email",
    lower(trim(col("email")))
)


# Clean state
silver_df = silver_df.withColumn(
    "state",
    upper(
        trim(
            coalesce(
                col("state"),
                lit("UNKNOWN")
            )
        )
    )
)


print("=== SILVER ===")

silver_df.show()


# Write Silver
silver_df.write \
    .mode("overwrite") \
    .parquet(
        "data/silver/customer.parquet"
    )


print("Silver written successfully.")


spark.stop()