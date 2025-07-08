from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, count as _count, avg, when, isnan, countDistinct, to_date
import boto3
from decimal import Decimal

# === CONFIG ===
BUCKET_NAME = "lab6ecs"
S3_PREFIX = "validated"
LOCAL_PREFIX = f"s3://{BUCKET_NAME}/{S3_PREFIX}"

DDB_ORDER_TABLE = "order_kpis"
DDB_CATEGORY_TABLE = "category_kpis"

# Initialize Spark
spark = SparkSession.builder.appName("TransformKPI").getOrCreate()
spark._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")

# Read CSVs
products = spark.read.option("header", True).csv(f"{LOCAL_PREFIX}/products/products.csv")
orders = spark.read.option("header", True).csv(f"{LOCAL_PREFIX}/orders/orders_part1.csv")
order_items = spark.read.option("header", True).csv(f"{LOCAL_PREFIX}/order_items/order_items_part1.csv")

# Join datasets
df = order_items.join(orders, on="order_id", how="inner") \
                .join(products, order_items["product_id"] == products["id"], how="inner")

# Convert to_date
df = df.withColumn("created_at_order", to_date("created_at_order")) \
       .withColumn("order_date", to_date("created_at_order"))

# === CATEGORY KPIs ===
cat_kpis = df.groupBy("category", "order_date").agg(
    _sum("sale_price").alias("daily_revenue"),
    avg("sale_price").alias("avg_order_value"),
    _count("order_id").alias("total_orders"),
    _sum(when(col("returned_at_item").isNotNull(), 1).otherwise(0)).alias("total_returns")
).withColumn("avg_return_rate", col("total_returns") / col("total_orders")) \
 .drop("total_returns", "total_orders")

cat_kpis.show()

# === ORDER KPIs ===
order_kpis = df.groupBy("order_date").agg(
    _countDistinct("order_id").alias("total_orders"),
    _sum("sale_price").alias("total_revenue"),
    _count("id_item").alias("total_items_sold"),
    _countDistinct("user_id_order").alias("unique_customers"),
    _sum(when(col("returned_at_item").isNotNull(), 1).otherwise(0)).alias("total_returns")
).withColumn("return_rate", col("total_returns") / col("total_orders")) \
 .drop("total_returns")

order_kpis.show()

# === Write to DynamoDB ===
dynamodb = boto3.resource("dynamodb")
order_table = dynamodb.Table(DDB_ORDER_TABLE)
category_table = dynamodb.Table(DDB_CATEGORY_TABLE)

for row in order_kpis.collect():
    order_table.put_item(Item={
        "order_date": str(row["order_date"]),
        "total_orders": int(row["total_orders"]),
        "total_revenue": Decimal(str(row["total_revenue"])),
        "total_items_sold": int(row["total_items_sold"]),
        "unique_customers": int(row["unique_customers"]),
        "return_rate": Decimal(str(row["return_rate"]))
    })

for row in cat_kpis.collect():
    category_table.put_item(Item={
        "category": str(row["category"]),
        "order_date": str(row["order_date"]),
        "daily_revenue": Decimal(str(row["daily_revenue"])),
        "avg_order_value": Decimal(str(row["avg_order_value"])),
        "avg_return_rate": Decimal(str(row["avg_return_rate"]))
    })

print("✅ Finished writing KPIs to DynamoDB")
