import boto3
import os
import json
import logging
from datetime import datetime
from botocore.exceptions import ClientError

# --- Logging Configuration ---
# Configure logging for the Lambda function
# Lambda automatically sends print statements to CloudWatch Logs, but using logging
# provides more control over log levels and formatting.
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- AWS Service Clients ---
# Initialize S3 client outside the handler to reuse connection across invocations (best practice)
s3 = boto3.client('s3')
# Get the S3 bucket name from environment variables, or use a hardcoded default
bucket_name = os.environ.get("BUCKET_NAME", "lab6ecs") 

def lambda_handler(event, context):
    """
    AWS Lambda function to archive processed S3 files.
    It expects a specific 'event' structure containing file paths to be archived.

    Args:
        event (dict): The event dictionary containing input file paths and metadata.
                      Expected structure:
                      {
                          "group_key": "unique_batch_identifier",
                          "order_date": "YYYY-MM-DD",
                          "orders_path": "path/to/orders_file.csv",
                          "order_items_path": "path/to/order_items_file.csv",
                          "products_path": "path/to/products_file.csv"
                      }
        context (object): The Lambda context object, providing runtime information.

    Returns:
        dict: A dictionary indicating the archiving status and input metadata.
    """
    logger.info(f"Lambda function triggered with event: {json.dumps(event)}")

    try:
        # Extract necessary information from the event payload
        group_key = event['group_key']
        order_date = event['order_date']
        paths = {
            "orders": event['orders_path'],
            "order_items": event['order_items_path'],
            "products": event['products_path']
        }
        logger.info(f"Processing group_key: {group_key}, order_date: {order_date}")

        # Iterate through each file type to archive
        for file_type, original_key in paths.items():
            # Extract filename from the original S3 key
            filename = os.path.basename(original_key)
            # Construct the new archive key using a structured path
            archive_key = f"archived/{file_type}/{order_date}_{group_key}_{filename}"

            logger.info(f"Attempting to archive: {original_key} to {archive_key}")
            
            try:
                # --- Copy Object to Archive Location ---
                s3.copy_object(
                    Bucket=bucket_name,
                    CopySource={'Bucket': bucket_name, 'Key': original_key},
                    Key=archive_key
                )
                logger.info(f"Successfully copied: {original_key} to {archive_key}")

                # --- Delete Original Object ---
                # Only delete if copy was successful
                s3.delete_object(Bucket=bucket_name, Key=original_key)
                logger.info(f"Successfully deleted original: {original_key}")

            except ClientError as e:
                # Log specific S3 errors during copy or delete operations
                logger.error(f"Failed to archive {original_key}: {e.response['Error']['Message']}")
                # Re-raise the exception to indicate failure to the caller (e.g., Step Functions)
                raise Exception(f"S3 operation failed for {original_key}: {e}")

        # Return success status if all files are processed
        logger.info(f"All files for group_key {group_key} archived successfully.")
        return {
            "status": "ARCHIVED",
            "group_key": group_key,
            "order_date": order_date
        }

    except KeyError as e:
        # Handle cases where expected keys are missing from the event payload
        logger.error(f"Event payload missing required key: {e}. Event: {json.dumps(event)}")
        raise Exception(f"Invalid event payload: Missing key {e}") from e
    except Exception as e:
        # Catch any other unexpected errors during execution
        logger.error(f"Archive failed for group_key {event.get('group_key', 'N/A')}: {str(e)}", exc_info=True)
        # Re-raise the exception so Lambda marks the invocation as a failure
        raise e