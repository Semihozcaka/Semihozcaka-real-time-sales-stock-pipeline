import dask.dataframe as dd
import os

RAW_DATA_DIR = os.path.join("data", "raw")

def load_data():
    print("[INFO] Loading sales and stock data with Dask...")

    sales = dd.read_csv(os.path.join(RAW_DATA_DIR, "sales.csv"), assume_missing=True)
    stock = dd.read_csv(os.path.join(RAW_DATA_DIR, "stock.csv"), assume_missing=True)

    print("[INFO] Data loaded successfully.")
    return sales, stock


def preprocess_sales(sales):
    print("[INFO] Preprocessing sales data...")

    sales["time"] = dd.to_datetime(sales["time"], errors="coerce")
    sales["quantity"] = sales["quantity"].astype("float")
    sales["price"] = sales["price"].astype("float")

    sales["date"] = sales["time"].dt.date

    return sales


def compute_sales_by_region(sales):
    print("[INFO] Computing total sales by region...")

    region_sales = sales.groupby("region")["quantity"].sum().compute()
    print(region_sales)

    return region_sales


def compute_daily_sales(sales):
    print("[INFO] Computing daily sales trend...")

    daily = sales.groupby("date")["quantity"].sum().compute()
    print(daily)

    return daily


def compute_daily_demand(sales):
    print("[INFO] Computing daily demand per product...")

    demand = sales.groupby(["product_id", "date"])["quantity"].sum().compute()
    print(demand)

    return demand


def compute_stock_alerts(stock, demand):
    print("[INFO] Detecting products with low stock compared to daily demand...")

    stock_latest = stock.compute()

    merged = demand.reset_index().merge(
        stock_latest,
        on="product_id",
        how="left"
    )

    merged["stock_alert"] = merged["current_stock"] < merged["quantity"]

    alerts = merged[merged["stock_alert"] == True]

    print(alerts.head())
    return alerts


def main():
    sales, stock = load_data()

    sales = preprocess_sales(sales)

    sales_by_region = compute_sales_by_region(sales)
    daily_sales = compute_daily_sales(sales)
    demand = compute_daily_demand(sales)
    alerts = compute_stock_alerts(stock, demand)

    print("[INFO] Dask processing completed.")


if __name__ == "__main__":
    main()
