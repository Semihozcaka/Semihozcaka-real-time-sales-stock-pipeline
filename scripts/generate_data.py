import os
import numpy as np
import pandas as pd
from faker import Faker

# ---------- Ayarlar ----------
NUM_SALES_ROWS = 50000   # Üretilecek satış satırı sayısı
NUM_STOCK_ROWS = 500     # Üretilecek stok satırı sayısı

RAW_DATA_DIR = os.path.join("data", "raw")

REGIONS = ["Marmara", "Ege", "Karadeniz", "İç Anadolu", "Doğu Anadolu", "Güneydoğu Anadolu"]

fake = Faker("tr_TR")


def ensure_directories():
    """Gerekli klasörlerin var olduğundan emin ol."""
    os.makedirs(RAW_DATA_DIR, exist_ok=True)


def generate_sales_data(num_rows: int) -> pd.DataFrame:
    """Satış verisini simüle eder."""
    product_ids = np.random.randint(1, 500, size=num_rows)
    customer_ids = np.random.randint(1, 10000, size=num_rows)
    regions = np.random.choice(REGIONS, size=num_rows)
    quantities = np.random.randint(1, 10, size=num_rows)
    prices = np.round(np.random.uniform(50, 2000, size=num_rows), 2)

    # Son 30 gün içinde rastgele zaman damgaları
    timestamps = [
        fake.date_time_between(start_date="-30d", end_date="now")
        for _ in range(num_rows)
    ]

    sales_df = pd.DataFrame(
        {
            "product_id": product_ids,
            "customer_id": customer_ids,
            "time": timestamps,
            "region": regions,
            "quantity": quantities,
            "price": prices,
        }
    )

    return sales_df


def generate_stock_data(num_rows: int) -> pd.DataFrame:
    """Stok verisini simüle eder."""
    product_ids = np.random.randint(1, 500, size=num_rows)
    warehouse_ids = np.random.randint(1, 20, size=num_rows)
    current_stocks = np.random.randint(0, 1000, size=num_rows)

    # Son 2 gün içinde rastgele güncelleme tarihleri
    update_times = [
        fake.date_time_between(start_date="-2d", end_date="now")
        for _ in range(num_rows)
    ]

    stock_df = pd.DataFrame(
        {
            "product_id": product_ids,
            "warehouse_id": warehouse_ids,
            "current_stock": current_stocks,
            "update_time": update_times,
        }
    )

    return stock_df


def save_dataframes(sales_df: pd.DataFrame, stock_df: pd.DataFrame):
    """DataFrame'leri hem CSV hem Parquet olarak kaydeder."""
    sales_csv_path = os.path.join(RAW_DATA_DIR, "sales.csv")
    sales_parquet_path = os.path.join(RAW_DATA_DIR, "sales.parquet")

    stock_csv_path = os.path.join(RAW_DATA_DIR, "stock.csv")
    stock_parquet_path = os.path.join(RAW_DATA_DIR, "stock.parquet")

    print(f"[INFO] Saving sales data to {sales_csv_path} and {sales_parquet_path}")
    sales_df.to_csv(sales_csv_path, index=False)
    sales_df.to_parquet(sales_parquet_path, index=False)

    print(f"[INFO] Saving stock data to {stock_csv_path} and {stock_parquet_path}")
    stock_df.to_csv(stock_csv_path, index=False)
    stock_df.to_parquet(stock_parquet_path, index=False)


def main():
    print("[INFO] Ensuring directories exist...")
    ensure_directories()

    print(f"[INFO] Generating {NUM_SALES_ROWS} rows of sales data...")
    sales_df = generate_sales_data(NUM_SALES_ROWS)

    print(f"[INFO] Generating {NUM_STOCK_ROWS} rows of stock data...")
    stock_df = generate_stock_data(NUM_STOCK_ROWS)

    print("[INFO] Saving data to disk...")
    save_dataframes(sales_df, stock_df)

    print("[INFO] Done. Raw data is ready under data/raw/")


if __name__ == "__main__":
    main()
