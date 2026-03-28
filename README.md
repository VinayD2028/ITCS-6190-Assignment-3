# Serverless Data Pipeline & Analytics Dashboard on AWS

An end-to-end cloud data engineering project built on AWS, demonstrating serverless ETL, data cataloging, SQL analytics, and a live web dashboard — all deployed using AWS free-tier services.

## Architecture Overview

```
S3 (raw/) ──► Lambda (ETL) ──► S3 (processed/) ──► Glue Crawler ──► Athena ──► EC2 Flask Dashboard
```

A CSV file uploaded to S3 automatically triggers a Lambda function that cleans and filters the data. The processed output is cataloged by AWS Glue, made queryable by Amazon Athena, and surfaced through a live Flask web application hosted on an EC2 instance.

## Tech Stack

| Layer | Service / Tool |
|---|---|
| Cloud Platform | Amazon Web Services (AWS) |
| Serverless Compute | AWS Lambda (Python 3.9) |
| Object Storage | Amazon S3 |
| Data Catalog | AWS Glue Crawler |
| SQL Analytics | Amazon Athena |
| Web Server | EC2 (Amazon Linux 2023) + Python Flask |
| Language | Python 3.x |
| Libraries | boto3, csv, io, datetime, Flask |

## Repository Structure

```
├── orders.csv         # Sample dataset (200 orders: OrderID, Customer, Amount, Status, OrderDate)
├── LambdaFunction.py  # AWS Lambda ETL handler — filters and writes processed CSV to S3
└── dashboard.py       # Flask web app — queries Athena and renders an analytics dashboard
```

## How It Works

**1. Data Ingestion (S3 + Lambda)**
Uploading `orders.csv` to the `raw/` folder in S3 automatically triggers the Lambda function via an S3 event notification. The function decodes the file, filters out orders that are both `pending`/`cancelled` *and* older than 30 days, then writes the cleaned dataset to the `processed/` folder as `filtered_orders.csv`.

**2. Data Cataloging (AWS Glue)**
A Glue Crawler scans the `processed/` folder and automatically infers the schema, registering it as a table (`filtered_orders`) in the `orders_db` data catalog database.

**3. SQL Analytics (Amazon Athena)**
Athena queries the Glue catalog to run standard SQL against the S3-stored CSV. Five analytical queries are defined:
- Total sales by customer
- Monthly order volume and revenue
- Order status breakdown (shipped vs. confirmed)
- Average order value (AOV) per customer
- Top 10 largest orders in February 2025

**4. Live Dashboard (EC2 + Flask)**
A Python Flask application hosted on a `t2.micro` EC2 instance calls each Athena query via `boto3`, polls for completion, fetches the results from S3, and renders a styled HTML table dashboard served on port 5000.

## Deployment Guide

### Prerequisites
- An AWS account with access to S3, Lambda, Glue, Athena, and EC2
- IAM roles configured for Lambda, Glue, and EC2 (see table below)

### IAM Roles Required

| Role Name | Service | Policies |
|---|---|---|
| `Lambda-S3-Processing-Role` | Lambda | AWSLambdaBasicExecutionRole, AmazonS3FullAccess |
| `Glue-S3-Crawler-Role` | Glue | AWSGlueServiceRole, AmazonS3ReadOnlyAccess |
| `EC2-Athena-Dashboard-Role` | EC2 | AmazonAthenaFullAccess, AmazonS3ReadOnlyAccess |

### S3 Bucket Structure

```
your-bucket/
├── raw/            ← Upload orders.csv here to trigger the pipeline
├── processed/      ← Lambda writes filtered_orders.csv here
└── athena-results/ ← Athena query output location
```

### Step-by-Step Deployment

**1. Create S3 Bucket** with the folder structure above.

**2. Deploy Lambda Function**
- Runtime: Python 3.9+
- Handler: `LambdaFunction.lambda_handler`
- Attach `Lambda-S3-Processing-Role`
- Add S3 trigger: prefix `raw/`, suffix `.csv`

**3. Set Up Glue Crawler**
- Data source: `s3://your-bucket/processed/`
- IAM role: `Glue-S3-Crawler-Role`
- Output database: `orders_db`

**4. Launch EC2 Instance**
- AMI: Amazon Linux 2023, instance type: `t2.micro`
- Attach `EC2-Athena-Dashboard-Role` as instance profile
- Open inbound port 5000 in the security group

**5. Install Dependencies on EC2**
```bash
sudo yum update -y
sudo yum install python3-pip -y
pip3 install flask boto3
```

**6. Configure and Run the Flask App**
```bash
nano dashboard.py   # Update AWS_REGION, ATHENA_DATABASE, S3_OUTPUT_LOCATION
python3 dashboard.py
```

**7. Start the Pipeline**
Upload `orders.csv` to `s3://your-bucket/raw/`. The Lambda function triggers automatically. Run the Glue Crawler, then visit `http://<EC2-Public-IP>:5000` to see the live dashboard.

## Key Concepts Demonstrated

- **Event-driven architecture**: S3 object creation triggers serverless compute with no polling or scheduling required
- **Serverless ETL**: Stateless data transformation using AWS Lambda with no servers to manage or scale
- **Data lake pattern**: Raw → Processed → Cataloged → Queryable separation of concerns across S3 prefixes
- **IAM least-privilege design**: Separate roles scoped to each service's minimum required permissions
- **Infrastructure reproducibility**: All resources defined with clear configurations that can be recreated from scratch
