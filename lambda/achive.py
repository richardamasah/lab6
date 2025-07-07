import boto3
import os
import json
from datetime import datetime

s3 = boto3.client('s3')
bucket_name = os.environ.get("BUCKET_NAME", "lab6ecs")  # or set hardcoded

def lambda_handler(event, context):
    try:
        group_key = event['group_key']
        order_date = event['order_date']
        paths = {
            "orders": event['orders_path'],
            "order_items": event['order_items_path'],
            "products": event['products_path']
        }

        for file_type, original_key in paths.items():
            filename = original_key.split('/')[-1]
            archive_key = f"archived/{file_type}/{order_date}_{group_key}_{filename}"

            # Copy and delete
            s3.copy_object(
                Bucket=bucket_name,
                CopySource={'Bucket': bucket_name, 'Key': original_key},
                Key=archive_key
            )
            s3.delete_object(Bucket=bucket_name, Key=original_key)

            print(f"✅ Archived: {original_key} → {archive_key}")

        return {
            "status": "ARCHIVED",
            "group_key": group_key,
            "order_date": order_date
        }

    except Exception as e:
        print(f"❌ Archive failed: {str(e)}")
        raise e
