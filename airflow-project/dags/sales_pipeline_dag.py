from datetime import datetime
from decimal import Decimal
import csv
import os
import logging

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.email import send_email


@dag(
    dag_id="sales_pipeline",
    description="CSV to PostgreSQL sales aggregation pipeline",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["homework", "sales"],
)
def sales_pipeline():

    @task()
    def check_csv():
        file_path = Variable.get(
            "sales_csv_path",
            default_var="/opt/airflow/data/sales.csv"
        )

        if not os.path.isfile(file_path):
            raise FileNotFoundError(f"CSV not found: {file_path}")

        required_columns = {
            "order_id", "order_date", "customer_id",
            "product", "category", "quantity", "unit_price"
        }

        with open(file_path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)

            if not reader.fieldnames:
                raise ValueError("CSV is empty or has no header")

            missing = required_columns - set(reader.fieldnames)
            if missing:
                raise ValueError(f"Missing columns: {missing}")

            rows = list(reader)


        seen_ids = set()

        for row in rows:
            order_id = row["order_id"].strip()

            if not order_id or order_id in seen_ids:
                raise ValueError(f"Missing or duplicate order_id: {order_id}")

            seen_ids.add(order_id)

            datetime.strptime(row["order_date"], "%Y-%m-%d")

            if not row["customer_id"].strip():
                raise ValueError("Missing customer_id")

            if not row["product"].strip() or not row["category"].strip():
                raise ValueError("Missing product or category")

            quantity = int(row["quantity"])
            price = Decimal(row["unit_price"])

            if quantity <= 0 or not price.is_finite() or price < 0:
                raise ValueError(f"Invalid quantity or price: {row}")

        logging.info("CSV validation successful. Rows: %s", len(rows))

        return {"file_path": file_path, "row_count": len(rows)}

    @task()
    def load_to_postgres(csv_info):
        hook = PostgresHook(postgres_conn_id="postgres_default")
        conn = hook.get_conn()

        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS sales (
                        order_id BIGINT PRIMARY KEY,
                        order_date DATE NOT NULL,
                        customer_id BIGINT NOT NULL,
                        product VARCHAR(255) NOT NULL,
                        category VARCHAR(100) NOT NULL,
                        quantity INTEGER NOT NULL CHECK (quantity > 0),
                        unit_price NUMERIC(12,2) NOT NULL
                            CHECK (unit_price >= 0)
                    )
                """)

                cursor.execute("DELETE FROM sales")

                with open(
                    csv_info["file_path"],
                    newline="",
                    encoding="utf-8"
                ) as f:
                    reader = csv.DictReader(f)

                    for row in reader:
                        cursor.execute("""
                            INSERT INTO sales (
                                order_id, order_date, customer_id,
                                product, category, quantity, unit_price
                            )
                            VALUES (%s, %s, %s, %s, %s, %s, %s)
                        """, (
                            int(row["order_id"]),
                            row["order_date"],
                            int(row["customer_id"]),
                            row["product"],
                            row["category"],
                            int(row["quantity"]),
                            Decimal(row["unit_price"]),
                        ))

            conn.commit()
            logging.info("Loaded %s rows", csv_info["row_count"])

        except Exception:
            conn.rollback()
            logging.exception("PostgreSQL loading failed")
            raise
        finally:
            conn.close()

        return csv_info["row_count"]

    @task()
    def aggregate_sales(loaded_rows):
        hook = PostgresHook(postgres_conn_id="postgres_default")
        conn = hook.get_conn()

        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS sales_aggregation (
                        order_date DATE NOT NULL,
                        category VARCHAR(100) NOT NULL,
                        order_count BIGINT NOT NULL,
                        total_quantity BIGINT NOT NULL,
                        total_sales NUMERIC(14,2) NOT NULL,
                        avg_unit_price NUMERIC(12,2) NOT NULL,
                        PRIMARY KEY (order_date, category)
                    )
                """)

                cursor.execute("TRUNCATE TABLE sales_aggregation")

                cursor.execute("""
                    INSERT INTO sales_aggregation (
                        order_date, category, order_count,
                        total_quantity, total_sales, avg_unit_price
                    )
                    SELECT
                        order_date,
                        category,
                        COUNT(*),
                        SUM(quantity),
                        SUM(quantity * unit_price),
                        AVG(unit_price)
                    FROM sales
                    GROUP BY order_date, category
                """)

                cursor.execute(
                    "SELECT COUNT(*) FROM sales_aggregation"
                )
                result_count = cursor.fetchone()[0]

            conn.commit()
            logging.info(
                "Loaded rows: %s, aggregation rows: %s",
                loaded_rows,
                result_count
            )

            return result_count

        except Exception:
            conn.rollback()
            logging.exception("Aggregation failed")
            raise
        finally:
            conn.close()

    @task()
    def check_aggregation(result_count):
        logging.info(
            "Received aggregation count via XCom: %s",
            result_count
        )

        if result_count == 0:
            recipient = os.getenv("AIRFLOW_ALERT_EMAIL", "").strip()

            if not recipient:
                raise ValueError(
                    "Set AIRFLOW_ALERT_EMAIL environment variable"
                )

            send_email(
                to=recipient,
                subject="Airflow Warning - Empty Aggregation Result",
                html_content=(
                    "The sales aggregation pipeline completed successfully, "
                    "but the final aggregation result contains 0 records. "
                    "Please investigate the input data and pipeline execution."
                ),
            )

            logging.warning("Empty aggregation alert sent")

        return "Pipeline completed" 
      


    csv_info = check_csv()
    loaded_rows = load_to_postgres(csv_info)
    aggregated_rows = aggregate_sales(loaded_rows)
    check_aggregation(aggregated_rows)


sales_pipeline()
