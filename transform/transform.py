import pandas as pd
import boto3
import os
import sys
import logging
from botocore.exceptions import ClientError
from decimal import Decimal

# --- Logging Configuration ---
# Configure logging to output messages to stdout with a timestamp and log level
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# === CONFIGURATION ===
# S3 bucket name for input and output
BUCKET_NAME = "lab6ecs"
# S3 prefix where validated data files are located
S3_PREFIX = "validated"
# Local directory within the container for temporary file storage
LOCAL_DIR = "/tmp"

# Dictionary defining the logical names of files and their S3 keys
FILES = {
    "products": "products/products.csv",
    "orders": "orders/orders_part1.csv",
    "order_items": "order_items/order_items_part1.csv"
}

# DynamoDB table names for storing calculated KPIs
DDB_ORDER_TABLE = "order_kpis"
DDB_CATEGORY_TABLE = "category_kpis"

def download_file(s3_client, s3_key, local_path):
    """
    Downloads a single file from S3 to a specified local path.
    Args:
        s3_client: Boto3 S3 client instance.
        s3_key (str): The full S3 key (e.g., 'validated/products/products.csv').
        local_path (str): The local file path to save the downloaded file.
    Raises:
        Exception: If the download fails due to a ClientError.
    """
    try:
        logger.info(f"Attempting to download: s3://{BUCKET_NAME}/{s3_key} to {local_path}")
        s3_client.download_file(BUCKET_NAME, s3_key, local_path)
        logger.info(f"Downloaded: {s3_key}")
    except ClientError as e:
        logger.error(f"Failed to download {s3_key}: {e}")
        # Re-raising a generic Exception as per original code structure
        raise Exception(f"Failed to download {s3_key}: {e}")

def write_order_kpis(dynamodb, df):
    """
    Writes Order-Level KPIs from a Pandas DataFrame to the DynamoDB Order KPI table.
    Args:
        dynamodb: Boto3 DynamoDB resource instance.
        df (pd.DataFrame): DataFrame containing order-level KPIs.
    Raises:
        Exception: If writing to DynamoDB fails due to a ClientError.
    """
    table = dynamodb.Table(DDB_ORDER_TABLE)
    logger.info(f"Writing Order-Level KPIs to DynamoDB table: {DDB_ORDER_TABLE}")
    for _, row in df.iterrows():
        try:
            item = {
                'order_date': str(row['order_date']),
                'total_orders': int(row['total_orders']),
                'total_revenue': Decimal(str(row['total_revenue'])), # Convert float to Decimal for DynamoDB
                'total_items_sold': int(row['total_items_sold']),
                'unique_customers': int(row['unique_customers']),
                'return_rate': Decimal(str(row['return_rate'])) # Convert float to Decimal for DynamoDB
            }
            table.put_item(Item=item)
            logger.debug(f"Wrote order KPI for {item['order_date']}") # Use debug for per-item logs
        except ClientError as e:
            logger.error(f"Failed to write order KPI for {row.get('order_date', 'N/A')} to DynamoDB: {e}")
            # Re-raising a generic Exception as per original code structure
            raise Exception(f"Failed to write order KPI to DynamoDB: {e}")
    logger.info("Finished writing Order-Level KPIs.")

def write_category_kpis(dynamodb, df):
    """
    Writes Category-Level KPIs from a Pandas DataFrame to the DynamoDB Category KPI table.
    Args:
        dynamodb: Boto3 DynamoDB resource instance.
        df (pd.DataFrame): DataFrame containing category-level KPIs.
    Raises:
        Exception: If writing to DynamoDB fails due to a ClientError.
    """
    table = dynamodb.Table(DDB_CATEGORY_TABLE)
    logger.info(f"Writing Category-Level KPIs to DynamoDB table: {DDB_CATEGORY_TABLE}")
    for _, row in df.iterrows():
        try:
            item = {
                'category': str(row['category']),
                'order_date': str(row['order_date']),
                'daily_revenue': Decimal(str(row['daily_revenue'])), # Convert float to Decimal for DynamoDB
                'avg_order_value': Decimal(str(row['avg_order_value'])), # Convert float to Decimal for DynamoDB
                'avg_return_rate': Decimal(str(row['avg_return_rate'])) # Convert float to Decimal for DynamoDB
            }
            table.put_item(Item=item)
            logger.debug(f"Wrote category KPI for {item['category']} on {item['order_date']}") # Use debug for per-item logs
        except ClientError as e:
            logger.error(f"Failed to write category KPI for {row.get('category', 'N/A')} on {row.get('order_date', 'N/A')} to DynamoDB: {e}")
            # Re-raising a generic Exception as per original code structure
            raise Exception(f"Failed to write category KPI to DynamoDB: {e}")
    logger.info("Finished writing Category-Level KPIs.")

def transform_and_store():
    """
    Performs data loading, merging, KPI calculations (Category and Order level),
    and stores the results in DynamoDB.
    """
    logger.info("Starting data transformation and storage process...")

    # --- Load DataFrames ---
    # Load products, orders, and order_items data from locally downloaded CSVs
    products = pd.read_csv(f"{LOCAL_DIR}/products.csv")
    orders = pd.read_csv(f"{LOCAL_DIR}/orders_part1.csv")
    order_items = pd.read_csv(f"{LOCAL_DIR}/order_items_part1.csv")
    logger.info("DataFrames loaded into Pandas.")

    # --- Merge DataFrames ---
    # Merge order_items with orders on 'order_id', appending '_item' and '_order' suffixes to common columns
    df = order_items.merge(orders, on="order_id", suffixes=("_item", "_order"))
    # Merge the combined DataFrame with products on 'product_id' (from order_items) and 'id' (from products)
    df = df.merge(products, left_on="product_id", right_on="id")
    logger.info("DataFrames merged successfully.")

    # --- Convert Date Columns ---
    # Convert 'created_at_order' to datetime objects, coercing errors to NaT (Not a Time)
    df['created_at_order'] = pd.to_datetime(df['created_at_order'], errors='coerce')
    # Extract just the date part from 'created_at_order' for grouping
    df['order_date'] = df['created_at_order'].dt.date
    logger.info("Date columns processed.")

    # === CATEGORY KPIs Calculation ===
    logger.info("Calculating Category-Level KPIs...")
    cat_kpis = df.groupby(['category', 'order_date']).agg(
        daily_revenue=pd.NamedAgg(column="sale_price", aggfunc="sum"), # Sum of sales price per category per day
        avg_order_value=pd.NamedAgg(column="sale_price", aggfunc="mean"), # Average sales price per category per day
        total_returns=pd.NamedAgg(column="returned_at_item", aggfunc=lambda x: x.notnull().sum()), # Count of non-null 'returned_at_item' to get total returns
        total_orders=pd.NamedAgg(column="order_id", aggfunc="count") # Count of order items which acts as total items processed
    ).reset_index() # Convert groupby object back to DataFrame
    # Calculate average return rate
    cat_kpis['avg_return_rate'] = cat_kpis['total_returns'] / cat_kpis['total_orders']
    # Drop intermediate columns
    cat_kpis.drop(columns=['total_returns', 'total_orders'], inplace=True)
    logger.info("Category-Level KPIs calculated.")
    logger.info("Category KPIs head:\n" + cat_kpis.head().to_string()) # Print head of KPIs for verification

    # === ORDER KPIs Calculation ===
    logger.info("Calculating Order-Level KPIs...")
    order_kpis = df.groupby('order_date').agg(
        total_orders=pd.NamedAgg(column="order_id", aggfunc=lambda x: x.nunique()), # Count of unique order IDs per day
        total_revenue=pd.NamedAgg(column="sale_price", aggfunc="sum"), # Total sales revenue per day
        total_items_sold=pd.NamedAgg(column="id_x", aggfunc="count"), # Total number of items sold per day (using 'id_x' after merge)
        total_returns=pd.NamedAgg(column="returned_at_item", aggfunc=lambda x: x.notnull().sum()), # Count of returned items per day
        unique_customers=pd.NamedAgg(column="user_id_order", aggfunc=lambda x: x.nunique()) # Count of unique user IDs per day
    ).reset_index() # Convert groupby object back to DataFrame
    # Calculate overall return rate
    order_kpis['return_rate'] = order_kpis['total_returns'] / order_kpis['total_orders']
    # Drop intermediate column
    order_kpis.drop(columns=['total_returns'], inplace=True)
    logger.info("Order-Level KPIs calculated.")
    logger.info("Order KPIs head:\n" + order_kpis.head().to_string()) # Print head of KPIs for verification

    # --- Store in DynamoDB ---
    dynamodb = boto3.resource('dynamodb')
    write_order_kpis(dynamodb, order_kpis)
    write_category_kpis(dynamodb, cat_kpis)
    logger.info("KPIs stored in DynamoDB.")

def main():
    """
    Main function to orchestrate the download, transformation, and storage process.
    Handles overall script execution and error management.
    """
    try:
        s3 = boto3.client("s3")
        # Ensure the local directory exists for downloads
        os.makedirs(LOCAL_DIR, exist_ok=True)
        logger.info(f"Local temporary directory created/ensured: {LOCAL_DIR}")

        # Download all necessary files from S3
        for logical_name, s3_key in FILES.items():
            # Construct local path, flattening the S3 key structure (e.g., products/products.csv -> /tmp/products.csv)
            local_path = os.path.join(LOCAL_DIR, os.path.basename(s3_key))
            # Construct the full S3 path by combining S3_PREFIX and the file's s3_key
            full_s3_path = f"{S3_PREFIX}/{s3_key}" # This line seems redundant with FILES structure. Assuming s3_key already includes relevant prefix parts.
                                                  # Original s3_key examples were 'products/products.csv', so full_s3_path would be 'validated/products/products.csv'.
            download_file(s3, full_s3_path, local_path)

        logger.info("Starting KPI Transformation & Upload...")
        transform_and_store() # Call the main transformation and storage logic
        logger.info("Done writing to DynamoDB")
        sys.exit(0) # Exit with success code

    except Exception as e:
        # Catch any exceptions that occur during the main execution flow
        logger.critical(f"Transform Failed: An unhandled error occurred: {e}", exc_info=True)
        sys.exit(1) # Exit with failure code

if __name__ == "__main__":
    main()