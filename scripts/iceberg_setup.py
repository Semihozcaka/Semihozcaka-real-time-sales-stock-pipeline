import os
import duckdb

RAW_DATA_DIR = os.path.join("data", "raw")
WAREHOUSE_DIR = "warehouse"

SALES_VERSION_DIR = os.path.join(WAREHOUSE_DIR, "sales_data_v1")
STOCK_VERSION_DIR = os.path.join(WAREHOUSE_DIR, "stock_data_v1")


def ensure_dirs():
    os.makedirs(WAREHOUSE_DIR, exist_ok=True)
    os.makedirs(SALES_VERSION_DIR, exist_ok=True)
    os.makedirs(STOCK_VERSION_DIR, exist_ok=True)


def setup_connection():
    db_path = os.path.join(WAREHOUSE_DIR, "lakehouse.duckdb")
    print(f"[INFO] Connecting to DuckDB at {db_path}")
    con = duckdb.connect(db_path)
    return con


def create_sales_table(con):
    print("[INFO] Creating partitioned sales dataset (region, date)...")

    con.execute(f"""
        CREATE OR REPLACE VIEW raw_sales AS
        SELECT
            product_id,
            customer_id,
            region,
            quantity::DOUBLE AS quantity,
            price::DOUBLE AS price,
            time::TIMESTAMP AS time,
            date_trunc('day', time::TIMESTAMP)::DATE AS date
        FROM read_csv_auto('{os.path.join(RAW_DATA_DIR, "sales.csv")}', header=True);
    """)

    # Partition by region + date (Iceberg'e benzer partition mantığı)
    con.execute(f"""
        COPY (
            SELECT * FROM raw_sales
        ) TO '{SALES_VERSION_DIR}'
        (FORMAT 'parquet', PARTITION_BY (region, date));
    """)

    print(f"[INFO] Sales data written as partitioned Parquet under {SALES_VERSION_DIR}")


def create_stock_table(con):
    print("[INFO] Creating stock dataset...")

    con.execute(f"""
        CREATE OR REPLACE VIEW raw_stock AS
        SELECT
            product_id,
            warehouse_id,
            current_stock::BIGINT AS current_stock,
            update_time::TIMESTAMP AS update_time
        FROM read_csv_auto('{os.path.join(RAW_DATA_DIR, "stock.csv")}', header=True);
    """)

    # Tek bir Parquet dosyasına yaz (klasör + dosya adı)
    stock_parquet_path = os.path.join(STOCK_VERSION_DIR, "stock.parquet")

    con.execute(f"""
        COPY (
            SELECT * FROM raw_stock
        ) TO '{stock_parquet_path}'
        (FORMAT 'parquet');
    """)

    print(f"[INFO] Stock data written as Parquet to {stock_parquet_path}")

def create_current_stock_view(con):
    print("[INFO] Creating 'current_stock' analytical view...")

    # Son güncelleme tarihine göre en güncel stok kaydını bul
    con.execute("""
        CREATE OR REPLACE VIEW current_stock AS
        SELECT
            s.product_id,
            s.warehouse_id,
            s.current_stock,
            s.update_time
        FROM read_parquet('warehouse/stock_data_v1/*.parquet') s
        QUALIFY s.update_time = max(s.update_time) OVER (PARTITION BY s.product_id, s.warehouse_id);
    """)

    print("[INFO] View 'current_stock' created in DuckDB (simulating Iceberg view).")


def list_versions():
    print("[INFO] Available 'versions' in warehouse (folder-based snapshots):")
    for name in os.listdir(WAREHOUSE_DIR):
        path = os.path.join(WAREHOUSE_DIR, name)
        if os.path.isdir(path) and name.endswith("_v1"):
            print(f"  - {name} -> {path}")


def main():
    ensure_dirs()
    con = setup_connection()

    create_sales_table(con)
    create_stock_table(con)
    create_current_stock_view(con)
    list_versions()

    print("[INFO] Iceberg-like warehouse setup completed.")


if __name__ == "__main__":
    main()
