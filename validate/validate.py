import pandas as pd
import boto3
import os
import sys
import logging
from botocore.exceptions import ClientError

# --- Logging Configuration ---
# Configure logging to output messages to stdout with a timestamp and log level
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Configuration Variables ---
# S3 bucket name where validated data resides
BUCKET_NAME = "lab6ecs"
# S3 prefix for validated input files within the bucket
S3_PREFIX = "validated"
# Local directory within the container to store downloaded files temporarily
LOCAL_DIR = "/tmp"

# Dictionary mapping local filenames to their S3 keys for download
REQUIRED_FILES = {
    "products.csv": f"{S3_PREFIX}/products/products.csv",
    "orders_part1.csv": f"{S3_PREFIX}/orders/orders_part1.csv",
    "order_items_part1.csv": f"{S3_PREFIX}/order_items/order_items_part1.csv"
}

def download_files_from_s3():
    """
    Downloads required data files from the specified S3 bucket to the local temporary directory.
    Raises ClientError if any download fails.
    """
    s3 = boto3.client('s3')
    logger.info("Starting file download from S3...")
    for filename, s3_key in REQUIRED_FILES.items():
        local_path = os.path.join(LOCAL_DIR, filename)
        try:
            # Attempt to download the file from S3
            s3.download_file(BUCKET_NAME, s3_key, local_path)
            logger.info(f"Downloaded: {filename} to {local_path}")
        except ClientError as e:
            # Log specific S3 client errors and re-raise to be caught by main handler
            logger.error(f"Failed to download {filename} from S3 (Key: {s3_key}): {e}")
            raise # Re-raise the exception to propagate failure

def validate_data():
    """
    Loads downloaded CSV files into pandas DataFrames and performs data validation checks.
    Exits with status 0 on success, 1 on validation failure.
    """
    logger.info("Running data validation checks...")
    try:
        # Load datasets from the local temporary directory
        products = pd.read_csv(f"{LOCAL_DIR}/products.csv")
        orders = pd.read_csv(f"{LOCAL_DIR}/orders_part1.csv")
        order_items = pd.read_csv(f"{LOCAL_DIR}/order_items_part1.csv")
        logger.info("All datasets loaded successfully for validation.")

        # Assertion 1: Check for essential columns in products data
        assert all(col_name in products.columns for col_name in ['id', 'sku', 'cost']), \
            "Missing essential columns in products data."
        logger.info("Products data column check passed.")

        # Assertion 2: Check referential integrity for order_id
        # Ensure all 'order_id' in order_items exist in orders
        assert set(order_items['order_id']).issubset(set(orders['order_id'])), \
            "Referential integrity failure: order_id mismatch between order_items and orders."
        logger.info("Order ID referential integrity check passed.")

        # Assertion 3: Check referential integrity for product_id
        # Ensure all 'product_id' in order_items exist in products
        assert set(order_items['product_id']).issubset(set(products['id'])), \
            "Referential integrity failure: product_id mismatch between order_items and products."
        logger.info("Product ID referential integrity check passed.")

        # Assertion 4: Validate 'num_of_item' in orders data
        # Ensure 'num_of_item' is positive
        assert orders['num_of_item'].min() > 0, "Validation failed: 'num_of_item' must be greater than 0."
        logger.info("'num_of_item' validation passed.")

        logger.info("Validation Passed: All data quality checks completed successfully.")
        sys.exit(0) # Exit with success code
    except Exception as e:
        # Catch any assertion errors or other exceptions during validation
        logger.error(f"Validation Failed: {e}")
        sys.exit(1) # Exit with failure code

if __name__ == "__main__":
    try:
        # Step 1: Download files from S3
        download_files_from_s3()
        # Step 2: Perform data validation
        validate_data()
    except Exception as e:
        # Catch any fatal errors that occurred during download or initial setup
        logger.critical(f"Fatal error during pipeline execution: {e}", exc_info=True)
        sys.exit(1) # Exit with failure code