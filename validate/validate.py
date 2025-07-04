import pandas as pd
import boto3
import os
import sys
from botocore.exceptions import ClientError

# Config
BUCKET_NAME = "lab6ecs"
S3_PREFIX = "raw"
LOCAL_DIR = "/tmp"

REQUIRED_FILES = [
    "products.csv",
    "orders_part1.csv",
    "order_items_part1.csv"
]

def download_files_from_s3():
    s3 = boto3.client('s3')
    for file_name in REQUIRED_FILES:
        s3_key = f"{S3_PREFIX}/{file_name}"
        local_path = os.path.join(LOCAL_DIR, file_name)
        try:
            s3.download_file(BUCKET_NAME, s3_key, local_path)
            print(f"✅ Downloaded: {file_name}")
        except ClientError as e:
            print(f"❌ Failed to download {file_name}: {e}")
            raise

def validate_data():
    try:
        products = pd.read_csv(f"{LOCAL_DIR}/products.csv")
        orders = pd.read_csv(f"{LOCAL_DIR}/orders_part1.csv")
        order_items = pd.read_csv(f"{LOCAL_DIR}/order_items_part1.csv")

        # Schema & logic checks
        assert all(col in products.columns for col in ['id', 'sku', 'cost']), "Missing product columns"
        assert set(order_items['order_id']).issubset(set(orders['order_id'])), "order_id mismatch"
        assert set(order_items['product_id']).issubset(set(products['id'])), "product_id mismatch"
        assert orders['num_of_item'].min() > 0, "Invalid num_of_item"

        print("✅ Validation Passed")
        sys.exit(0)
    except Exception as e:
        print(f"❌ Validation Failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    try:
        print("🚀 Starting download from S3...")
        download_files_from_s3()
        print("🔍 Running data validation...")
        validate_data()
    except Exception as e:
        print(f"💥 Fatal error: {e}")
        sys.exit(1)
