import os
import pandas as pd
from dotenv import load_dotenv
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

RAW_DATA_DIR = os.path.join("data", "raw")


def load_env():
    """Load Snowflake credentials from .env file."""
    load_dotenv()
    cfg = {
        "user": os.getenv("SNOWFLAKE_USER"),
        "password": os.getenv("SNOWFLAKE_PASSWORD"),
        "account": os.getenv("SNOWFLAKE_ACCOUNT"),
        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
        "database": os.getenv("SNOWFLAKE_DATABASE"),
        "schema": os.getenv("SNOWFLAKE_SCHEMA"),
    }
    missing = [k for k, v in cfg.items() if not v]
    if missing:
        raise ValueError(f"Missing Snowflake config keys in .env: {missing}")
    return cfg


def connect_snowflake(cfg):
    print("[INFO] Connecting to Snowflake...")
    conn = snowflake.connector.connect(
        user=cfg["user"],
        password=cfg["password"],
        account=cfg["account"],
        warehouse=cfg["warehouse"],
        database=cfg["database"],
        schema=cfg["schema"],
    )
    print("[INFO] Connected.")
    return conn


def ensure_objects(cursor, cfg):
    print("[INFO] Ensuring database, schema and tables exist...")

    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {cfg['database']}")
    cursor.execute(f"USE DATABASE {cfg['database']}")

    cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {cfg['schema']}")
    cursor.execute(f"USE SCHEMA {cfg['schema']}")

    cursor.execute(f"""
        CREATE OR REPLACE TABLE SALES (
            PRODUCT_ID      NUMBER,
            CUSTOMER_ID     NUMBER,
            TIME            TIMESTAMP,
            REGION          STRING,
            QUANTITY        FLOAT,
            PRICE           FLOAT
        );
    """)

    cursor.execute(f"""
        CREATE OR REPLACE TABLE STOCK (
            PRODUCT_ID      NUMBER,
            WAREHOUSE_ID    NUMBER,
            CURRENT_STOCK   NUMBER,
            UPDATE_TIME     TIMESTAMP
        );
    """)

    print("[INFO] Tables SALES and STOCK are ready.")


def load_local_data():
    print("[INFO] Loading local CSV data into pandas...")
    sales_path = os.path.join(RAW_DATA_DIR, "sales.csv")
    stock_path = os.path.join(RAW_DATA_DIR, "stock.csv")

    sales_df = pd.read_csv(sales_path)
    stock_df = pd.read_csv(stock_path)

    print(f"[INFO] Sales rows: {len(sales_df)}, Stock rows: {len(stock_df)}")
    return sales_df, stock_df


def upload_to_snowflake(conn, sales_df, stock_df):
    print("[INFO] Uploading pandas DataFrames to Snowflake tables...")

    sales_success, sales_chunks, sales_rows, _ = write_pandas(
        conn, sales_df, "SALES", quote_identifiers=False
    )
    print(f"[INFO] SALES upload success={sales_success}, rows={sales_rows}, chunks={sales_chunks}")

    stock_success, stock_chunks, stock_rows, _ = write_pandas(
        conn, stock_df, "STOCK", quote_identifiers=False
    )
    print(f"[INFO] STOCK upload success={stock_success}, rows={stock_rows}, chunks={stock_chunks}")


def main():
    cfg = load_env()
    conn = connect_snowflake(cfg)
    cur = conn.cursor()

    try:
        ensure_objects(cur, cfg)
        sales_df, stock_df = load_local_data()
        upload_to_snowflake(conn, sales_df, stock_df)
        print("[INFO] Snowflake load completed.")
    finally:
        cur.close()
        conn.close()
        print("[INFO] Connection closed.")


if __name__ == "__main__":
    main()
