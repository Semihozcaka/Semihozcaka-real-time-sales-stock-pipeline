import pandas as pd

df = pd.read_csv("data/raw/sales.csv")

# Time kolonunu STR (text) haline getir
df["time"] = df["time"].astype(str)

df.to_csv("data/raw/sales_snowflake_v2.csv", index=False)

print("Wrote data/raw/sales_snowflake_v2.csv")
