import pandas as pd
import sys

try:
    DATA_DIR = "/app/data"

    # Load all datasets
    products = pd.read_csv(f"{DATA_DIR}/products.csv")
    orders = pd.concat([pd.read_csv(f"{DATA_DIR}/orders_part{i}.csv") for i in range(1, 7)])
    order_items = pd.concat([pd.read_csv(f"{DATA_DIR}/order_items_part{i}.csv") for i in range(1, 17)])

    # Schema + logic checks (you can add more)
    assert set(order_items['order_id']).issubset(set(orders['order_id'])), "Broken order_id link"
    assert set(order_items['product_id']).issubset(set(products['id'])), "Broken product_id link"

    print("✅ Validation Passed")
    sys.exit(0)

except Exception as e:
    print(f"❌ Validation Failed: {e}")
    sys.exit(1)
