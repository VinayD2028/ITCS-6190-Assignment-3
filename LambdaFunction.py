import boto3
import csv
import io
from datetime import datetime, timedelta
import urllib.parse

# Initialize the S3 client using the Lambda execution role's IAM permissions.
# No credentials are hardcoded — boto3 picks them up from the environment automatically.
s3 = boto3.client('s3')

def lambda_handler(event, context):
    """
    AWS Lambda entry point. Triggered by an S3 PutObject event when a file
    is uploaded to the raw/ prefix of the configured bucket.

    Filtering logic:
        - Orders with status 'pending' or 'cancelled' AND an OrderDate older
          than 30 days are removed from the dataset.
        - All other orders (active statuses, or recent pending/cancelled) are kept.

    Output:
        Writes a filtered CSV to the processed/ folder in the same S3 bucket.
    """
    print("Lambda triggered by S3 event.")

    # Extract the source bucket name and object key from the S3 event payload.
    # S3 URL-encodes special characters (e.g., spaces as '+'), so we decode them.
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    key_from_event = event['Records'][0]['s3']['object']['key']
    raw_key = urllib.parse.unquote_plus(key_from_event, encoding='utf-8')

    # Isolate just the filename (e.g., "orders.csv") from the full S3 key path.
    file_name = raw_key.split('/')[-1]

    print(f"Incoming file: {raw_key}")

    # --- Step 1: Download the raw CSV from S3 into memory ---
    try:
        response = s3.get_object(Bucket=bucket_name, Key=raw_key)
        # Read the binary body, decode to a UTF-8 string, and split into lines
        # so that csv.DictReader can iterate over them without a file handle.
        raw_csv = response['Body'].read().decode('utf-8').splitlines()
        print(f"Successfully read file from S3: {file_name}")
    except Exception as e:
        print(f"Error reading file from S3: {e}")
        raise e  # Re-raise so Lambda marks the invocation as failed

    # --- Step 2: Parse and filter the CSV rows ---
    reader = csv.DictReader(raw_csv)  # Treats the first line as the header row
    filtered_rows = []
    original_count = 0
    filtered_out_count = 0

    # Any order older than this date AND with a non-active status will be dropped.
    cutoff_date = datetime.now() - timedelta(days=30)

    print("Processing records...")
    for row in reader:
        original_count += 1

        # Normalize the status string to avoid case-sensitivity issues.
        order_status = row['Status'].strip().lower()
        order_date = datetime.strptime(row['OrderDate'], "%Y-%m-%d")

        # Keep the row if:
        #   - The status is NOT pending/cancelled (i.e., it's shipped or confirmed), OR
        #   - The order is recent enough (within the last 30 days), regardless of status.
        if order_status not in ['pending', 'cancelled'] or order_date > cutoff_date:
            filtered_rows.append(row)
        else:
            filtered_out_count += 1

    print(f"Total records processed: {original_count}")
    print(f"Records filtered out: {filtered_out_count}")
    print(f"Records kept: {len(filtered_rows)}")

    # --- Step 3: Write the filtered rows to an in-memory CSV buffer ---
    # Using io.StringIO avoids writing to disk (Lambda has limited /tmp storage).
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=reader.fieldnames)
    writer.writeheader()       # Preserve the original column headers
    writer.writerows(filtered_rows)

    # --- Step 4: Upload the processed file to the processed/ S3 prefix ---
    processed_key = f"processed/filtered_{file_name}"

    try:
        s3.put_object(Bucket=bucket_name, Key=processed_key, Body=output.getvalue())
        print(f"Filtered file successfully written to S3: {processed_key}")
    except Exception as e:
        print(f"Error writing filtered file to S3: {e}")
        raise e

    # Return a standard Lambda HTTP-style response for logging/debugging purposes.
    return {
        'statusCode': 200,
        'body': f"Filtered {len(filtered_rows)} rows and saved to {processed_key}"
    }
