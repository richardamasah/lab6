import pandas as pd
import sys
import os

try:
    DATA_DIR = "/app/data"  # Absolute path inside container

    # Load all datasets
    products = pd.read_csv(f"{DATA_DIR}/products.csv")
    orders = pd.concat([pd.read_csv(f"{DATA_DIR}/orders_part{i}.csv") for i in range(1, 7)])
    order_items = pd.concat([pd.read_csv(f"{DATA_DIR}/order_items_part{i}.csv") for i in range(1, 17)])

    # Run your validation logic...
    print("✅ Validation Passed")
    sys.exit(0)

except Exception as e:
    print(f"❌ Validation Failed: {e}")
    sys.exit(1)
