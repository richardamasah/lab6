import pandas as pd
import sys
import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, count as _count, avg, when, isnan, countDistinct, to_date
import boto3
from decimal import Decimal
from botocore.exceptions import ClientError

# --- Logging Configuration ---
# Configure logging to output messages to stdout with a timestamp and log level
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

logger.info("Starting data transformation script.")

# --- CONFIG ---
# S3 bucket name where validated data resides
BUCKET_NAME = "lab6ecs"
# S3 prefix for validated input files
S3_PREFIX = "validated"
# Full S3 path for input data
LOCAL_PREFIX = f"s3://{BUCKET_NAME}/{S3_PREFIX}"

# DynamoDB table names for KPI storage
DDB_ORDER_TABLE = "order_kpis"
DDB_CATEGORY_TABLE = "category_kpis"

try:
    # Initialize Spark Session
    # Set application name for Spark UI and logs
    spark = SparkSession.builder.appName("TransformKPI").getOrCreate()
    # Configure Spark to use the Default AWS Credentials Provider Chain for S3 access
    spark._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
    logger.info("SparkSession initialized successfully.")

    # --- Read CSVs from S3 ---
    logger.info(f"Attempting to read data from S3 bucket: {LOCAL_PREFIX}")
    products = spark.read.option("header", True).csv(f"{LOCAL_PREFIX}/products/products.csv")
    orders = spark.read.option("header", True).csv(f"{LOCAL_PREFIX}/orders/orders_part1.csv") # Assuming single file for now based on code
    order_items = spark.read.option("header", True).csv(f"{LOCAL_PREFIX}/order_items/order_items_part1.csv") # Assuming single file for now based on code
    logger.info("Successfully loaded products, orders, and order_items datasets.")

    # --- Join datasets ---
    # Join order_items with orders on 'order_id' (inner join to keep only matching records)
    # Then join the result with products on 'product_id' (from order_items) and 'id' (from products)
    df = order_items.join(orders, on="order_id", how="inner") \
                    .join(products, order_items["product_id"] == products["id"], how="inner")
    logger.info("Datasets joined successfully.")
    
    # --- Convert timestamp columns to date type ---
    # Convert 'created_at_order' to date format and rename to 'order_date' for consistent grouping
    df = df.withColumn("created_at_order", to_date("created_at_order")) \
           .withColumn("order_date", to_date("created_at_order"))
    logger.info("Date columns converted.")

    # === CATEGORY KPIs Calculation ===
    logger.info("Calculating Category-Level KPIs...")
    cat_kpis = df.groupBy("category", "order_date").agg(
        _sum("sale_price").alias("daily_revenue"), # Total revenue per category per day
        avg("sale_price").alias("avg_order_value"), # Average sale price (order item value) per category per day
        _count("order_id").alias("total_orders"), # Total order items count per category per day (proxy for orders)
        _sum(when(col("returned_at_item").isNotNull(), 1).otherwise(0)).alias("total_returns") # Count of returned items
    ).withColumn("avg_return_rate", col("total_returns") / col("total_orders")) \
     .drop("total_returns", "total_orders") # Drop intermediate columns
    logger.info("Category-Level KPIs calculated.")
    cat_kpis.show() # Display Category KPIs (for local testing/debugging)

    # === ORDER KPIs Calculation ===
    logger.info("Calculating Order-Level KPIs...")
    order_kpis = df.groupBy("order_date").agg(
        _countDistinct("order_id").alias("total_orders"), # Count of unique orders per day
        _sum("sale_price").alias("total_revenue"), # Total revenue from all orders per day
        _count("id_item").alias("total_items_sold"), # Total number of items sold per day
        _countDistinct("user_id_order").alias("unique_customers"), # Number of distinct customers per day
        _sum(when(col("returned_at_item").isNotNull(), 1).otherwise(0)).alias("total_returns") # Total returns for the day
    ).withColumn("return_rate", col("total_returns") / col("total_orders")) \
     .drop("total_returns") # Drop intermediate column
    logger.info("Order-Level KPIs calculated.")
    order_kpis.show() # Display Order KPIs (for local testing/debugging)

    # === Write KPIs to DynamoDB ===
    logger.info("Attempting to write KPIs to DynamoDB...")
    dynamodb = boto3.resource("dynamodb")
    order_table = dynamodb.Table(DDB_ORDER_TABLE)
    category_table = dynamodb.Table(DDB_CATEGORY_TABLE)

    # Write Order-Level KPIs
    try:
        for row in order_kpis.collect():
            order_table.put_item(Item={
                "order_date": str(row["order_date"]),
                "total_orders": int(row["total_orders"]),
                "total_revenue": Decimal(str(row["total_revenue"])), # Convert float to Decimal for DynamoDB
                "total_items_sold": int(row["total_items_sold"]),
                "unique_customers": int(row["unique_customers"]),
                "return_rate": Decimal(str(row["return_rate"])) # Convert float to Decimal for DynamoDB
            })
        logger.info(f"Successfully wrote Order-Level KPIs to {DDB_ORDER_TABLE}.")
    except ClientError as e:
        logger.error(f"Failed to write Order-Level KPIs to DynamoDB: {e.response['Error']['Message']}")
        sys.exit(1) # Exit with error code

    # Write Category-Level KPIs
    try:
        for row in cat_kpis.collect():
            category_table.put_item(Item={
                "category": str(row["category"]),
                "order_date": str(row["order_date"]),
                "daily_revenue": Decimal(str(row["daily_revenue"])), # Convert float to Decimal for DynamoDB
                "avg_order_value": Decimal(str(row["avg_order_value"])), # Convert float to Decimal for DynamoDB
                "avg_return_rate": Decimal(str(row["avg_return_rate"])) # Convert float to Decimal for DynamoDB
            })
        logger.info(f"Successfully wrote Category-Level KPIs to {DDB_CATEGORY_TABLE}.")
    except ClientError as e:
        logger.error(f"Failed to write Category-Level KPIs to DynamoDB: {e.response['Error']['Message']}")
        sys.exit(1) # Exit with error code

    logger.info("✅ Finished writing KPIs to DynamoDB")
    sys.exit(0) # Exit successfully

except Exception as e:
    # Catch any unexpected errors during Spark processing or file reading
    logger.error(f" Transformation Failed: An unexpected error occurred: {e}", exc_info=True)
    sys.exit(1) # Exit with a non-zero code to indicate failure