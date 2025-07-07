# Data Pipeline - Testing Guide

## **Overview**

This guide provides detailed instructions for testing a Python-based data pipeline designed for processing and transforming customer data. The pipeline leverages various tools and technologies, including Kafka, Docker, and GitHub Actions.

## Key Features & Benefits
• **Data Streaming**: Utilizes Kafka for real-time data ingestion and processing
• **Modular Design**: Well-structured codebase with distinct modules for processing customer, product, and transaction data
• **Data Validation**: Implements schema validation to ensure data quality and integrity
• **Anonymization**: Includes data anonymization techniques to protect sensitive information
• **Monitoring**: Integrated with Prometheus and Grafana for monitoring pipeline performance and health
• **Containerization**: Dockerized for easy deployment and scalability
• **Continuous Integration**: Automated CI pipeline using GitHub Actions for testing

## **Prerequisites & Dependencies**

Ensure you have the following tools and libraries installed before you begin:
• Python: Version 3.9 or higher
• Docker: Latest version
• Docker Compose: Latest version
• Kafka: For data streaming
• Prometheus: For metrics collection
• Grafana: For dashboarding and visualization
• Git: For repository cloning

The project dependencies are listed in the requirements.txt file.

## Installation & Setup Instructions

#### 1. Clone the Repository:

git clone https://github.com/ramahrk/data-pipeline.git
cd data-pipeline

#### 2. Create and Activate Virtual Environment (Recommended):

sudo apt install python3-venv
python3 -m venv venv
source venv/bin/activate  # On Linux/macOS
venv/Scripts/activate  # On Windows

#### 3. Install Dependencies:

pip install -r requirements.txt

#### 4. Prepare Script Files:

Convert line endings and set file permissions:

dos2unix run_kafka_producer.sh
dos2unix run_kafka_consumer.sh
dos2unix create_kafka_topics.sh
chmod +x run_kafka_producer.sh
chmod +x run_kafka_consumer.sh
chmod +x create_kafka_topics.sh

If dos2unix is not installed, run:
sudo apt update
sudo apt install dos2unix

### **Local Testing with Existing Test Data****

#### **Step 1: Link Test Data to Input Directory**

Create the input directory structure and copy your test data:

mkdir -p data/input
# cp -r /testdata/* data/input/
ls data/input/

#### **Step 2: Run the Pipeline with Docker**

Using Docker Compose: Build and start the containers:

docker compose build
docker compose up -d

#### **Step 3: Run the pipeline for a specific date from your test data:
****
**# Replace 2020-01-20 with the actual date in your test data**
docker compose exec data-pipeline-etl-new python -m src.pipeline --start-date 2020-01-20 --end-date 2020-01-20

#### **Step 4: Run the pipeline for a specific date with metrics port::**

**# Replace 2020-01-20 with the actual date in your test data**
docker compose exec data-pipeline-etl-new python -m src.pipeline --start-date 2020-01-20 --end-date 2020-01-20 --metrics-port 8082

(Metrics are pushed to Pushgateway)

#### **Step 5: Check the logs:**

docker compose logs -f data-pipeline-etl-new

#### **Step 6: Testing GDPR Compliance - Anonymization Process**

**Check Existing Erasure Requests:**

docker compose run --rm data-pipeline-etl-new bash -c "find ./data/input -name 'erasure-requests.json.gz' | xargs -I{} zcat {} | head -n 5"

**Find Erasure Request Files:**

docker compose run --rm data-pipeline-etl-new bash -c "find ./data/input -name 'erasure-requests.json.gz'"

**Create Matching Customer Data for Erasure Requests:**

cd data-pipeline
docker compose run --rm data-pipeline-etl-new python ./create_matching_customers.py

This script will:
• Read all erasure requests from the input data
• Extract customer IDs and email addresses
• Generate matching customer records
• Save these records to the appropriate input directories for processing

**You can also specify specific dates:**

docker compose run --rm data-pipeline-etl-new python ./create_matching_customers.py --dates 2020-01-25 2020-01-25

**Process the Customer Data: After creating the matching customer data, process it with the pipeline: (Run ETL for the next day - 2020-01-26)**

docker compose run --rm data-pipeline-etl-new python -m src.pipeline --start-date 2020-01-26 --end-date 2020-01-26

**Verify Anonymization Results: Check the reference data to see if the customer records have been anonymized:**

docker-compose run --rm data-pipeline-etl-new bash -c "find /app/data/reference/customers -name '*.json' | xargs -I{} cat {}"

This should show customer records with anonymized fields (first name, last name).

**Check the Erasure Request Statistics:**

docker-compose run --rm data-pipeline-etl-new bash -c "find /app/data/output -name 'erasure-requests_stats.json' | xargs -I{} cat {}"

**What Just Happened?**
You ran a data generator script (create_matching_customers.py) that:
1. Found erasure requests for 2020-01-25
2. Created input data for 2020-01-24. Because erasure requests from 2020-01-25 must be applied when processing 2020-01-26
3. Run ETL for the next day (2020-01-26)

**This will:**
• Process data from 2020-01-26 (even if it's empty)
• Apply erasure requests from 2020-01-25 to the reference data (which includes 2020-01-24 input)

#### Step 7 Run Unit Tests with Docker compose:

docker compose run --rm data-pipeline-etl-new pytest -xvs tests/unit/

#### Step 8 Check the Results - Examine Processed Data:Step 8 Check the Results - Examine Processed Data:

**# List output directories**
ls data/output/

**# Check output files for a specific date (replace with actual date from your test data)**
ls ./data/output/date=2020-01-23/hour=00/

**# Install jq if not available**
sudo apt install jq

**# View processed customers**
gzip -dc ./data/output/date=2020-01-24/hour=00/customers.json.gz | jq

**# View processed products**
gzip -dc ./data/output/date=2020-01-25/hour=00/products.json.gz | jq

**# View processed transactions**
gzip -dc ./data/output/date=2020-01-25/hour=00/transactions.json.gz | jq

**##### Check Quarantined Data: -List quanrantine directories**
ls ./data/quarantine

**# Check quarantined files for a specific date**
ls ./data/quarantine/date=2020-01-23/hour=00/

**# View statistics for a specific date and hour**
cat ./data/output/date=2020-01-23/hour=00/products_stats.json

**# View aggregated statistics**
ls data/output/stats/
cat data/output/stats/stats_2020-01-23_all.json

**##### **Check Logs:****
cat logs/pipeline.log

#### Step 9 Test Streaming Functionality

**Start Kafka and related services:**

docker compose up -d zookeeper kafka

**Create Kafka topics:**

./create_kafka_topics.sh

**Run the streaming producer:**

./run_kafka_producer.sh

**Run the streaming consumer:** -**# In a separate terminal**
In a seperate terminal

./run_kafka_consumer.sh

#### **Step 10 Access Monitoring Dashboards**

**Access Prometheus:**
• Open http://localhost:9090 in your browser

**Access Grafana:**
• Open http://localhost:3000 in your browser
• Login with username `admin` and password `admin`
• Navigate to the ETL Pipeline dashboard

**Access Metrics: - Push Gateway**
• Open http://localhost:9091 in your browser

#### **Step 11 Test with CI -datapipeline-ci.yml**

**To execute the Continuous Integration workflow for testing the data pipeline:**
1. Navigate to the GitHub repository
2. Select the "Actions" tab
3. Choose the "Workflow" section
4. Click on "Run workflow"

**Important Note**: When testing the datapipeline-ci.yml workflow, ensure that only 2 or 3 folders are present in the data/input directory to facilitate expedited testing of the pipeline. This optimization significantly reduces the execution time of the CI workflow while still providing comprehensive validation of the pipeline's functionality.

#### Troubleshooting

**Issue: Can't access test data**

Check permissions:

ls /data/input
sudo chmod -R +r /data/input

**Check the logs:**

cat logs/pipeline.log

**Verify input data exists and has the expected structure:**

find data/input -type f | sort

**Check Docker container status:**

docker compose ps

**Issue: Kafka connectivity problems**

Check Kafka is running:
docker compose ps kafka

**Test Kafka connectivity:**

docker compose exec kafka kafka-topics --list --bootstrap-server kafka:29092

#### Metrics Overview

**Generic ETL Metrics**
• `etl_records_processed_total` (Counter): Number of records processed
• `etl_errors_total` (Counter): Total ETL errors

**Kafka Streaming Metrics**
• `kafka_messages_consumed_total` (Counter): Total Kafka messages consumed
• `kafka_messages_processed_total` (Counter): Total Kafka messages processed
• `kafka_valid_total` (Counter): Total valid Kafka messages
• `kafka_invalid_total` (Counter): Total invalid Kafka messages
• `kafka_message_processing_duration_seconds` (Histogram): Kafka message processing duration
• `kafka_messages_produced_total` (Counter): Total Kafka messages produced
• `kafka_messages_failed_total` (Counter): Total Kafka message produce failures
• `message_production_duration_seconds` (Histogram): Time taken to produce messages to Kafka

**Product Processing Metrics**
• `products_processed_total` (Counter): Total product records processed
• `products_valid_total` (Counter): Total valid product records
• `products_invalid_total` (Counter): Total invalid product records
• `products_processing_duration_seconds` (Histogram): Product processing duration in seconds

**Erasure Request (GDPR) Metrics**
• `erasure_requests_processed_total` (Counter): Total erasure requests processed
• `erasure_requests_failed_total` (Counter): Total failed erasure requests
• `erasure_requests_successful_total` (Counter): Total successful erasure requests
• `records_anonymized_total` (Counter): Total records anonymized
• `erasure_request_processing_duration_seconds` (Histogram): Erasure request processing duration in seconds

**Transaction Metrics**
• `transactions_processed_total` (Counter): Total transaction records processed
• `transactions_valid_total` (Counter): Total valid transaction records
• `transactions_invalid_total` (Counter): Total invalid transaction records
• `transactions_processing_duration_seconds` (Histogram): Transaction processing duration in seconds

**Additional Information**

These metrics are collected via Prometheus and can be visualized through Grafana dashboards.

For optimal monitoring, it is recommended to establish alerts for critical metrics such as elevated error rates, processing delays, or failed erasure requests. The metrics can be accessed through the Prometheus interface at http://localhost:9090, through the Pushgateway at http://localhost:9091, or through the configured Grafana dashboards at http://localhost:3000.

#### Known Issues

**Product SKU Not Found Warning in Transaction Processing**

**Issue**: Transaction records may log a warning like:

WARNING: Product with SKU PROD123 not found in reference data

**Cause**: The referenced product SKU in a transaction doesn't exist in the product reference dataset.
**Impact**: The transaction is considered invalid and routed to quarantine.
**Fix**: Create the product generation script to create mock reference data before transaction processing.Ensure all referenced products are present in `/data/reference/products`

**hour=00 Directory Doesn't Show Logs**

**Issue**: Logs or outputs for hour=00 sometimes don't display properly or are skipped in aggregation when running:

docker compose run --rm data-pipeline-etl-new python -m src.pipeline --start-date 2020-01-26 --hour 00

