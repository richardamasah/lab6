import boto3
import csv
import json
from io import StringIO
from urllib.parse import unquote_plus
from datetime import datetime, timedelta
import os
import logging

# 🔧 Setup
logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')
stepfunctions = boto3.client('stepfunctions')

DDB_TABLE = os.environ['DDB_TABLE']  # ingestion_registry
STEP_FUNCTION_ARN = os.environ['STEP_FUNCTION_ARN']
DEBOUNCE_SECONDS = int(os.environ.get('DEBOUNCE_SECONDS', 120))

table = dynamodb.Table(DDB_TABLE)

# Expected headers
REQUIRED_HEADERS = {
    'orders': ['order_id', 'user_id', 'status', 'created_at', 'returned_at', 'shipped_at', 'delivered_at', 'num_of_item'],
    'order_items': ['id', 'order_id', 'user_id', 'product_id', 'status', 'created_at', 'shipped_at', 'delivered_at', 'returned_at', 'sale_price'],
    'products': ['id', 'sku', 'cost', 'category', 'name', 'brand', 'retail_price', 'department']
}

def lambda_handler(event, context):
    records = event.get('Records')
    if not records:
        return {"validation_status": "FAILED", "reason": "No records to process"}

    for record in records:
        bucket = record['s3']['bucket']['name']
        key = unquote_plus(record['s3']['object']['key'])
        logger.info(f"📁 Processing file: s3://{bucket}/{key}")

        try:
            obj = s3.get_object(Bucket=bucket, Key=key)
            lines = obj['Body'].read().decode('utf-8').splitlines()
            reader = csv.reader(lines)
            headers = next(reader, None)

            if headers is None:
                raise Exception("File has no headers")

            file_type = detect_file_type(key)
            group_key = extract_group_key(key)

            if file_type not in REQUIRED_HEADERS:
                raise Exception(f"Unsupported file type: {file_type}")

            if not set(REQUIRED_HEADERS[file_type]).issubset(set(headers)):
                handle_rejected_file(bucket, key, file_type, "Missing required headers")
                continue

            # Handle products.csv separately
            if file_type == "products":
                products_dest = "validated/products/products.csv"
                s3.copy_object(Bucket=bucket, CopySource={'Bucket': bucket, 'Key': key}, Key=products_dest)
                s3.delete_object(Bucket=bucket, Key=key)

                table.update_item(
                    Key={"group_key": "latest_products"},
                    UpdateExpression="SET products_path = :p",
                    ExpressionAttributeValues={":p": products_dest}
                )
                logger.info(f"🟢 Products file updated to: {products_dest}")
                continue

            # Grab first data row for date
            data_row = next(reader, None)
            if data_row is None:
                raise Exception("No data rows found")

            order_date = extract_order_date(headers, data_row)

            update_registry(group_key, file_type, key, order_date)

        except Exception as e:
            logger.error(f"❌ Error in file {key}: {str(e)}")
            handle_rejected_file(bucket, key, "unknown", str(e))

    return {"validation_status": "SUCCESS"}

def detect_file_type(key):
    if "orders" in key:
        return "orders"
    elif "order_items" in key:
        return "order_items"
    elif "products" in key:
        return "products"
    return "unknown"

def extract_group_key(key):
    filename = key.split('/')[-1]
    return filename.replace(".csv", "").split("_")[-1]  # e.g. part1

def extract_order_date(headers, row):
    try:
        idx = headers.index("created_at")
        return datetime.strptime(row[idx][:10], "%Y-%m-%d").strftime("%Y-%m-%d")
    except:
        return datetime.utcnow().strftime("%Y-%m-%d")

def update_registry(group_key, file_type, path, order_date):
    update_expr = "SET #fp = :path, #flag = :true, order_date = :od"
    expr_names = {
        "#fp": f"{file_type}_path",
        "#flag": f"has_{file_type}"
    }
    expr_values = {
        ":path": path,
        ":true": True,
        ":od": order_date
    }

    response = table.update_item(
        Key={"group_key": group_key},
        UpdateExpression=update_expr,
        ExpressionAttributeNames=expr_names,
        ExpressionAttributeValues=expr_values,
        ReturnValues="ALL_NEW"
    )

    item = response.get('Attributes', {})
    if item.get("has_orders") and item.get("has_order_items"):
        logger.info(f"🚀 Triggering Step Function for group: {group_key}")
        stepfunctions.start_execution(
            stateMachineArn=STEP_FUNCTION_ARN,
            input=json.dumps({
                "group_key": group_key,
                "order_date": order_date,
                "orders_path": item.get("orders_path"),
                "order_items_path": item.get("order_items_path"),
                "products_path": item.get("products_path", "validated/products/products.csv")
            })
        )
    else:
        ttl = int((datetime.utcnow() + timedelta(seconds=DEBOUNCE_SECONDS)).timestamp())
        table.update_item(
            Key={"group_key": group_key},
            UpdateExpression="SET debounce_ttl = :ttl",
            ExpressionAttributeValues={":ttl": ttl}
        )
        logger.info(f"🕒 Debounce TTL set for group {group_key}: {ttl}")

def handle_rejected_file(bucket, key, file_type, reason):
    rejected_key = key.replace("raw/", f"rejected/{file_type}/")
    s3.copy_object(Bucket=bucket, CopySource={'Bucket': bucket, 'Key': key}, Key=rejected_key)
    s3.delete_object(Bucket=bucket, Key=key)
    s3.put_object(
        Bucket=bucket,
        Key=rejected_key.replace(".csv", "_reason.json"),
        Body=json.dumps({"reason": reason, "original_key": key}),
        ContentType="application/json"
    )
    logger.warning(f"❌ File rejected and moved to: {rejected_key}")
