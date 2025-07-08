import boto3
import csv
import json
from io import StringIO
from urllib.parse import unquote_plus
from datetime import datetime, timedelta
import os
import logging
from botocore.exceptions import ClientError # Import ClientError for more specific handling

# --- Setup Logging ---
# Configure the root logger to output INFO level messages to CloudWatch Logs
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# --- AWS Service Clients ---
# Initialize S3, DynamoDB, and Step Functions clients.
# These are initialized globally to be reused across Lambda invocations, improving performance.
s3 = boto3.client('s3')
dynamodb = boto3.resource('dynamodb') # Use resource for easier table interaction
stepfunctions = boto3.client('stepfunctions')

# --- Configuration Variables ---
# DynamoDB table name for tracking ingestion status (e.g., 'ingestion_registry')
DDB_TABLE = os.environ.get('DDB_TABLE') # Use .get() for safer environment variable access
# AWS Step Functions State Machine ARN to trigger once a group of files is ready
STEP_FUNCTION_ARN = os.environ.get('STEP_FUNCTION_ARN')
# Debounce period in seconds: how long to wait for all files in a group before potentially triggering
DEBOUNCE_SECONDS = int(os.environ.get('DEBOUNCE_SECONDS', 120))

# Initialize DynamoDB table resource
table = dynamodb.Table(DDB_TABLE)

# --- Expected File Headers ---
# Defines the set of required headers for each file type to ensure data quality
REQUIRED_HEADERS = {
    'orders': ['order_id', 'user_id', 'status', 'created_at', 'returned_at', 'shipped_at', 'delivered_at', 'num_of_item'],
    'order_items': ['id', 'order_id', 'user_id', 'product_id', 'status', 'created_at', 'shipped_at', 'delivered_at', 'returned_at', 'sale_price'],
    'products': ['id', 'sku', 'cost', 'category', 'name', 'brand', 'retail_price', 'department']
}

def lambda_handler(event, context):
    """
    Main Lambda handler function triggered by S3 PUT events.
    It processes incoming CSV files, validates headers, moves files,
    updates a DynamoDB registry, and triggers a Step Functions workflow
    when a complete set of files for a 'group_key' is available.

    Args:
        event (dict): The Lambda event object, typically an S3 event.
        context (object): The Lambda context object.

    Returns:
        dict: A status indicating success or failure of the processing.
    """
    records = event.get('Records')
    if not records:
        logger.warning("No records found in the event. Skipping processing.")
        return {"validation_status": "FAILED", "reason": "No records to process"}

    for record in records:
        bucket = record['s3']['bucket']['name']
        # Decode S3 key to handle spaces or special characters in filenames
        key = unquote_plus(record['s3']['object']['key'])
        logger.info(f"Processing file: s3://{bucket}/{key}")

        try:
            # --- Fetch file content from S3 ---
            obj = s3.get_object(Bucket=bucket, Key=key)
            # Read file content, decode from UTF-8, and split into lines
            lines = obj['Body'].read().decode('utf-8').splitlines()
            # Use csv.reader to parse CSV content
            reader = csv.reader(lines)
            # Read the header row
            headers = next(reader, None)

            if headers is None:
                logger.error(f"File {key} has no headers. Rejecting.")
                raise Exception("File has no headers")

            # --- Detect file type and extract group key ---
            file_type = detect_file_type(key)
            group_key = extract_group_key(key)
            logger.info(f"Detected file type: {file_type}, Group Key: {group_key}")

            if file_type not in REQUIRED_HEADERS:
                logger.error(f"Unsupported file type detected for {key}: {file_type}. Rejecting.")
                raise Exception(f"Unsupported file type: {file_type}")

            # --- Validate headers ---
            if not set(REQUIRED_HEADERS[file_type]).issubset(set(headers)):
                missing_headers = set(REQUIRED_HEADERS[file_type]) - set(headers)
                reason = f"Missing required headers: {', '.join(missing_headers)}"
                logger.warning(f"File {key} rejected: {reason}")
                handle_rejected_file(bucket, key, file_type, reason)
                continue # Move to the next record if headers are missing

            # --- Special handling for 'products' file ---
            # Products file is a single, continuously updated reference dataset.
            # It's copied to a fixed 'validated/products/products.csv' path.
            if file_type == "products":
                validated_key = "validated/products/products.csv"
                logger.info(f"Handling products file: {key}. Copying to {validated_key} and deleting original.")
                s3.copy_object(Bucket=bucket, CopySource={'Bucket': bucket, 'Key': key}, Key=validated_key)
                s3.delete_object(Bucket=bucket, Key=key)

                # Update DynamoDB registry for the latest products file path
                table.update_item(
                    Key={"group_key": "latest_products"}, # Use a fixed group_key for products
                    UpdateExpression="SET products_path = :p",
                    ExpressionAttributeValues={":p": validated_key}
                )
                logger.info(f"Products file successfully updated to: {validated_key}")
                continue # Process next record

            # --- Extract order date from data row ---
            data_row = next(reader, None) # Read the first data row
            if data_row is None:
                logger.error(f"File {key} has no data rows after headers. Rejecting.")
                raise Exception("No data rows found")

            order_date = extract_order_date(headers, data_row)
            logger.info(f"Extracted order date: {order_date} for group: {group_key}")

            # --- Copy to 'validated' folder and delete original ---
            # Construct the validated S3 key (e.g., 'validated/orders/orders_part1.csv')
            validated_key = f"validated/{file_type}/{os.path.basename(key)}"
            logger.info(f"Copying {key} to {validated_key} and deleting original.")
            s3.copy_object(Bucket=bucket, CopySource={'Bucket': bucket, 'Key': key}, Key=validated_key)
            s3.delete_object(Bucket=bucket, Key=key)

            # --- Update Ingestion Registry in DynamoDB ---
            # This function updates the status for the current group_key and checks if Step Functions should be triggered.
            update_registry(group_key, file_type, validated_key, order_date)

        except Exception as e:
            # Catch any error during processing a single file and move it to 'rejected'
            logger.error(f"Error processing file {key}: {str(e)}", exc_info=True)
            handle_rejected_file(bucket, key, file_type if 'file_type' in locals() else "unknown", str(e))

    logger.info("Finished processing all records in the event.")
    return {"validation_status": "SUCCESS"}

def detect_file_type(key):
    """
    Determines the file type (orders, order_items, products) based on keywords in the S3 key.
    Args:
        key (str): The S3 object key.
    Returns:
        str: The detected file type or "unknown".
    """
    if "order_items" in key:
        return "order_items"
    elif "orders" in key:
        return "orders"
    elif "products" in key:
        return "products"
    return "unknown"

def extract_group_key(key):
    """
    Extracts a group key from the S3 object key, typically used to group related files (e.g., 'part1').
    Args:
        key (str): The S3 object key.
    Returns:
        str: The extracted group key.
    """
    filename = key.split('/')[-1]
    # Assumes filename format like 'orders_part1.csv'
    return filename.replace(".csv", "").split("_")[-1]

def extract_order_date(headers, row):
    """
    Extracts the 'created_at' date from a data row based on its header.
    Args:
        headers (list): List of column headers.
        row (list): List of data values for a row.
    Returns:
        str: The extracted date in 'YYYY-MM-DD' format, or current UTC date if 'created_at' is not found or invalid.
    """
    try:
        idx = headers.index("created_at")
        # Extract first 10 characters for date part and format
        return datetime.strptime(row[idx][:10], "%Y-%m-%d").strftime("%Y-%m-%d")
    except ValueError: # Catch if 'created_at' not in headers or date parsing fails
        logger.warning("Could not extract 'created_at' date. Using current UTC date.")
        return datetime.utcnow().strftime("%Y-%m-%d")
    except IndexError: # Catch if row[idx] is out of bounds
        logger.warning("Index error when extracting 'created_at' date. Using current UTC date.")
        return datetime.utcnow().strftime("%Y-%m-%d")

def update_registry(group_key, file_type, validated_path, order_date):
    """
    Updates the DynamoDB ingestion registry for a given group key and file type.
    It checks if all necessary files for a group are present and triggers Step Functions
    if debounce conditions are met, otherwise sets a TTL for debouncing.
    Args:
        group_key (str): The unique identifier for the group of files.
        file_type (str): The type of file being registered (orders, order_items).
        validated_path (str): The S3 key of the validated file.
        order_date (str): The order date associated with the files.
    """
    # Define update expressions and attribute values for DynamoDB
    update_expr = "SET #fp = :path, #flag = :true, order_date = :od"
    expr_names = {
        "#fp": f"{file_type}_path", # e.g., orders_path, order_items_path
        "#flag": f"has_{file_type}" # e.g., has_orders, has_order_items
    }
    expr_values = {
        ":path": validated_path,
        ":true": True,
        ":od": order_date
    }

    try:
        # Update the DynamoDB item for the given group_key
        response = table.update_item(
            Key={"group_key": group_key},
            UpdateExpression=update_expr,
            ExpressionAttributeNames=expr_names,
            ExpressionAttributeValues=expr_values,
            ReturnValues="ALL_NEW" # Return all attributes of the item after update
        )
        item = response.get('Attributes', {})
        logger.info(f"DynamoDB registry updated for group {group_key}: {file_type}={validated_path}")

        # Check if all required files (orders, order_items, and products) are present for this group
        # The products_path is expected to be present in the 'latest_products' item,
        # but here it is checked as part of the group's item for Step Function input.
        # This assumes 'products_path' is also copied to the group's item by some mechanism
        # or fetched separately. Given the products special handling, it's fetched from 'latest_products'
        # within update_registry logic.
        products_path_item = table.get_item(Key={"group_key": "latest_products"}).get('Item', {})
        products_path = products_path_item.get("products_path")
        
        # Trigger Step Function if all files are registered and products_path is available
        if item.get("has_orders") and item.get("has_order_items") and products_path:
            logger.info(f"All files ({group_key}) and products data are ready. Triggering Step Function for group: {group_key}")
            # Start the Step Functions execution with relevant input parameters
            stepfunctions.start_execution(
                stateMachineArn=STEP_FUNCTION_ARN,
                input=json.dumps({
                    "group_key": group_key,
                    "order_date": order_date,
                    "orders_path": item.get("orders_path"),
                    "order_items_path": item.get("order_items_path"),
                    "products_path": products_path # Use the products path from 'latest_products'
                })
            )
            logger.info(f"Step Function execution started for group: {group_key}")
            
            # Clean up the registry item for the group after triggering SF
            table.delete_item(Key={"group_key": group_key})
            logger.info(f"Registry item deleted for group: {group_key}")

        else:
            # If not all files are present, set a TTL for debouncing
            if not products_path:
                logger.info(f"Products file is not yet available. Waiting before triggering group: {group_key}")
            
            # Calculate TTL for the DynamoDB item to automatically expire after debounce period
            ttl = int((datetime.utcnow() + timedelta(seconds=DEBOUNCE_SECONDS)).timestamp())
            table.update_item(
                Key={"group_key": group_key},
                UpdateExpression="SET debounce_ttl = :ttl",
                ExpressionAttributeValues={":ttl": ttl}
            )
            logger.info(f"Debounce TTL set for group {group_key} until {datetime.fromtimestamp(ttl)} UTC.")

    except ClientError as e:
        logger.error(f"DynamoDB or Step Functions operation failed for group {group_key}: {e}")
        raise # Re-raise to be caught by the main handler

def handle_rejected_file(bucket, key, file_type, reason):
    """
    Moves a rejected file from its original location to a 'rejected' S3 prefix
    and adds a JSON file explaining the reason for rejection.
    Args:
        bucket (str): The S3 bucket name.
        key (str): The original S3 object key.
        file_type (str): The detected type of the file.
        reason (str): The reason for rejection.
    """
    # Construct the destination key for the rejected file
    # Replace 'raw/' prefix (assumed) with 'rejected/file_type/'
    rejected_key = key.replace("raw/", f"rejected/{file_type}/")
    # If 'raw/' is not in key, then it will simply prepend 'rejected/file_type/'
    if not rejected_key.startswith("rejected/"):
        rejected_key = f"rejected/{file_type}/{key.split('/')[-1]}" # Fallback if no raw/ prefix

    logger.warning(f"Rejecting file: {key}. Moving to {rejected_key} with reason: {reason}")
    try:
        # Copy the original file to the rejected location
        s3.copy_object(Bucket=bucket, CopySource={'Bucket': bucket, 'Key': key}, Key=rejected_key)
        # Delete the original file
        s3.delete_object(Bucket=bucket, Key=key)
        # Upload a JSON file explaining the rejection reason
        s3.put_object(
            Bucket=bucket,
            Key=rejected_key.replace(".csv", "_reason.json"), # Create a reason file next to the rejected CSV
            Body=json.dumps({"reason": reason, "original_key": key, "timestamp": datetime.utcnow().isoformat()}),
            ContentType="application/json"
        )
        logger.warning(f"File rejected and moved to: {rejected_key}")
    except ClientError as e:
        logger.error(f"Failed to move rejected file {key} to {rejected_key}: {e}")
    except Exception as e:
        logger.error(f"An unexpected error occurred during rejection handling for {key}: {e}")