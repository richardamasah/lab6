import pandas as pd
import sys

try:
    # Load all datasets
    products = pd.read_csv("data/products.csv")
    orders = pd.concat([pd.read_csv(f"data/orders_part{i}.csv") for i in range(1, 7)])
    order_items = pd.concat([pd.read_csv(f"data/order_items_part{i}.csv") for i in range(1, 17)])

    # BASIC CHECKS
    assert all(col in products.columns for col in ['id', 'sku', 'cost', 'category']), "Missing columns in products"
    assert all(orders['num_of_item'] > 0), "Invalid num_of_item"
    assert pd.api.types.is_numeric_dtype(products['retail_price']), "retail_price must be numeric"

    # REFERENTIAL INTEGRITY
    assert set(order_items['order_id']).issubset(set(orders['order_id'])), "Broken order_id link"
    assert set(order_items['product_id']).issubset(set(products['id'])), "Broken product_id link"

    print("✅ Validation Passed")
    sys.exit(0)

except Exception as e:
    print(f"❌ Validation Failed: {e}")
    sys.exit(1)
