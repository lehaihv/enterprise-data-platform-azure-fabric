from pyspark.sql import SparkSession


spark = (
    SparkSession.builder
    .appName("RetailSilver")
    .master("local[*]")
    .getOrCreate()
)


customer_df = spark.read.parquet(
    "data/bronze/customer.parquet"
)

product_df = spark.read.parquet(
    "data/bronze/product.parquet"
)

order_df = spark.read.parquet(
    "data/bronze/sales_order.parquet"
)

order_item_df = spark.read.parquet(
    "data/bronze/order_item.parquet"
)


print("Customer:", customer_df.count())
print("Product:", product_df.count())
print("Orders:", order_df.count())
print("Order items:", order_item_df.count())


spark.stop()