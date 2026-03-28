import boto3
import time
from flask import Flask

# --- CONFIGURATION — UPDATE THESE BEFORE RUNNING ---
AWS_REGION = "us-east-1"                                # AWS region where your resources are deployed
ATHENA_DATABASE = "default"                             # The Glue/Athena database name (e.g., "orders_db")
S3_OUTPUT_LOCATION = "s3://your-athena-results-bucket/" # S3 path where Athena writes query result CSVs
# ---------------------------------------------------

# Initialize the Flask web application.
app = Flask(__name__)

# Create boto3 clients for Athena and S3.
# The EC2 instance profile (IAM role) grants these clients the necessary permissions
# automatically — no credentials need to be hardcoded here.
athena_client = boto3.client('athena', region_name=AWS_REGION)
s3_client = boto3.client('s3', region_name=AWS_REGION)

# Define the five analytical queries to display on the dashboard.
# Each entry is a dict with a human-readable title and a SQL query string.
# The table name "filtered_orders" corresponds to the table created by the Glue Crawler
# from the processed/ S3 folder.
queries_to_run = [
    {
        "title": "1. Total Sales by Customer",
        # Ranks customers by their total spend — useful for identifying top buyers.
        "query": """
            SELECT Customer, SUM(Amount) AS TotalAmountSpent
            FROM "filtered_orders"
            GROUP BY Customer
            ORDER BY TotalAmountSpent DESC;
        """
    },
    {
        "title": "2. Monthly Order Volume and Revenue",
        # Truncates OrderDate to the month boundary, then aggregates — shows revenue trends over time.
        "query": """
            SELECT DATE_TRUNC('month', CAST(OrderDate AS DATE)) AS OrderMonth,
                   COUNT(OrderID) AS NumberOfOrders,
                   ROUND(SUM(Amount), 2) AS MonthlyRevenue
            FROM "filtered_orders"
            GROUP BY 1 ORDER BY OrderMonth;
        """
    },
    {
        "title": "3. Order Status Dashboard",
        # Summarizes the dataset by fulfillment status — shows how many orders are shipped vs confirmed.
        "query": """
            SELECT Status, COUNT(OrderID) AS OrderCount, ROUND(SUM(Amount), 2) AS TotalAmount
            FROM "filtered_orders"
            GROUP BY Status;
        """
    },
    {
        "title": "4. Average Order Value (AOV) per Customer",
        # AOV = total spend / number of orders — a standard e-commerce KPI per customer.
        "query": """
            SELECT Customer, ROUND(AVG(Amount), 2) AS AverageOrderValue
            FROM "filtered_orders"
            GROUP BY Customer
            ORDER BY AverageOrderValue DESC;
        """
    },
    {
        "title": "5. Top 10 Largest Orders in February 2025",
        # Filters to a specific month and returns the highest-value individual orders.
        "query": """
            SELECT OrderDate, OrderID, Customer, Amount
            FROM "filtered_orders"
            WHERE CAST(OrderDate AS DATE) BETWEEN DATE '2025-02-01' AND DATE '2025-02-28'
            ORDER BY Amount DESC LIMIT 10;
        """
    }
]


def run_athena_query(query):
    """
    Submits a SQL query to Amazon Athena and blocks until it completes.

    Athena is asynchronous — start_query_execution returns immediately with an execution ID.
    We poll get_query_execution every second until the status is terminal
    (SUCCEEDED, FAILED, or CANCELLED).

    On success, Athena writes results as a CSV to S3_OUTPUT_LOCATION.
    We then fetch and parse that CSV directly from S3 to avoid an extra API call.

    Returns:
        (header, results): A list of column names and a list of rows (each row is a list of strings).
        (None, error_message): If the query failed or an exception was raised.
    """
    try:
        # Submit the query to Athena. Results will be stored at S3_OUTPUT_LOCATION.
        response = athena_client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={'Database': ATHENA_DATABASE},
            ResultConfiguration={'OutputLocation': S3_OUTPUT_LOCATION}
        )
        query_execution_id = response['QueryExecutionId']

        # Poll Athena until the query reaches a terminal state.
        while True:
            stats = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
            status = stats['QueryExecution']['Status']['State']
            if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
                break
            time.sleep(1)  # Avoid hammering the API; wait 1 second between polls

        if status == 'SUCCEEDED':
            # Athena stores results as a CSV at the OutputLocation.
            # Parse the S3 path into bucket + key components.
            s3_path = stats['QueryExecution']['ResultConfiguration']['OutputLocation']
            bucket_name, key = s3_path.replace("s3://", "").split("/", 1)

            # Download the result CSV from S3.
            s3_response = s3_client.get_object(Bucket=bucket_name, Key=key)
            lines = s3_response['Body'].read().decode('utf-8').splitlines()

            # Athena wraps column names and values in double-quotes — strip them for display.
            header = [h.strip('"') for h in lines[0].split(',')]
            results = [[val.strip('"') for val in line.split(',')] for line in lines[1:]]
            return header, results
        else:
            # Surface the reason Athena rejected or cancelled the query.
            error_message = stats['QueryExecution']['Status'].get('StateChangeReason', 'Unknown error')
            return None, f"Query failed: {error_message}"

    except Exception as e:
        return None, f"An exception occurred: {str(e)}"


@app.route('/')
def index():
    """
    The root Flask route — the dashboard home page.

    Iterates over all queries in queries_to_run, executes each one via Athena,
    and builds an HTML page with a styled table for each result set.
    Errors are displayed inline in red so the dashboard remains usable
    even if one query fails.
    """
    # Build the HTML page header with inline CSS styling.
    html_content = "<html><head><title>Athena Orders Dashboard</title>"
    html_content += """
    <style>
        body { font-family: sans-serif; margin: 2em; background-color: #f4f4f9; }
        h1 { color: #333; }
        h2 { color: #555; border-bottom: 2px solid #ddd; padding-bottom: 5px; }
        table { border-collapse: collapse; width: 80%; margin-top: 20px; box-shadow: 0 2px 5px rgba(0,0,0,0.1); }
        th, td { border: 1px solid #ccc; padding: 10px; text-align: left; }
        th { background-color: #007bff; color: white; }
        tr:nth-child(even) { background-color: #f2f2f2; }
    </style>
    </head><body>"""
    html_content += "<h1>📊 Athena Orders Dashboard</h1>"

    # Render each query as a separate section with a heading and a data table.
    for item in queries_to_run:
        html_content += f"<h2>{item['title']}</h2>"

        # Execute the Athena query and wait for results.
        header, results = run_athena_query(item['query'])

        if header and results is not None:
            # Build an HTML table from the returned headers and rows.
            html_content += "<table><thead><tr>"
            for col in header:
                html_content += f"<th>{col}</th>"
            html_content += "</tr></thead><tbody>"
            for row in results:
                html_content += "<tr>"
                for cell in row:
                    html_content += f"<td>{cell}</td>"
                html_content += "</tr>"
            html_content += "</tbody></table>"
        else:
            # Display the error message inline rather than crashing the whole page.
            html_content += f"<p style='color:red;'><strong>Error:</strong> {results}</p>"

    html_content += "</body></html>"
    return html_content


if __name__ == '__main__':
    # Bind to 0.0.0.0 so the app is reachable from outside the EC2 instance
    # (not just from localhost). Port 5000 must be open in the security group.
    app.run(host='0.0.0.0', port=5000)
