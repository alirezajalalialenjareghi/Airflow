# Sales Data Pipeline with Apache Airflow

## Project Overview
This project implements an ETL pipeline using Apache Airflow and PostgreSQL to process sales data from a CSV file.

## Pipeline Tasks
1. **check_csv** – Checks whether the CSV file exists and validates its contents.
2. **load_to_postgres** – Loads validated sales data into PostgreSQL using the `postgres_default` Airflow connection.
3. **aggregate_sales** – Aggregates sales by order date and category and stores the results in `sales_aggregation`.
4. **check_aggregation** – Reads the aggregation count using XCom and sends an email alert if the result is empty.

## Technologies
- Apache Airflow 2.10.5
- PostgreSQL 16
- Docker Compose
- Python

## Project Structure
- `dags/` – Airflow DAG
- `data/` – Sample sales CSV file
- `screenshots/` – Execution evidence and logs
- `docker-compose.yaml` – Local development environment

## Test Scenarios
- Successful pipeline execution
- Empty CSV and email alert
- Missing CSV file
- PostgreSQL connection failure
- Successful recovery and reload of 10 records

## Email Alert
Subject: `Airflow Warning - Empty Aggregation Result`

The alert is sent when the aggregation result contains zero records.

## Running the Project
Start the environment with:

```bash
docker compose up -d
```

Open the Airflow web interface through port `8080` in GitHub Codespaces.
