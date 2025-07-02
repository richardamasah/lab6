import pandas as pd
import sys
from datetime import datetime

def parse_date(date_str):
    try:
        return pd.to_datetime(date_str)
    except:
        return pd.NaT

try:
    DATA_DIR = "/app/data"

    # Load files
    products = pd.read_csv(f"{DATA_DIR}/products.csv")
    orders = pd.concat([pd.read_csv(f"{DATA_DIR}/orders_part{i}.csv") for i in range(1, 7)])
    order_items = pd.concat([pd.read_csv(f"{DATA_DIR}/order_items_part{i}.csv") for i in range(1, 17)])

    # Merge datasets
    df = order_items.merge(orders, on="order_id", suffixes=("_item", "_order"))
    df = df.merge(products, left_on="product_id", right_on="id")

    # Convert dates
    df['created_at_order'] = df['created_at_order'].apply(parse_date)
    df['order_date'] = df['created_at_order'].dt.date


    ### --- CATEGORY KPIs ---
    cat_kpis = df.groupby(['category', 'order_date']).agg(
        daily_revenue=pd.NamedAgg(column="sale_price", aggfunc="sum"),
        avg_order_value=pd.NamedAgg(column="sale_price", aggfunc="mean"),
        total_returns=pd.NamedAgg(column="returned_at", aggfunc=lambda x: x.notnull().sum()),
        total_orders=pd.NamedAgg(column="order_id", aggfunc="count")
    ).reset_index()

    cat_kpis['avg_return_rate'] = cat_kpis['total_returns'] / cat_kpis['total_orders']
    cat_kpis.drop(columns=['total_returns', 'total_orders'], inplace=True)

    print("✅ Category KPIs:")
    print(cat_kpis.head())

    ### --- ORDER KPIs ---
    order_kpis = df.groupby('order_date').agg(
        total_orders=pd.NamedAgg(column="order_id", aggfunc=lambda x: x.nunique()),
        total_revenue=pd.NamedAgg(column="sale_price", aggfunc="sum"),
        total_items_sold=pd.NamedAgg(column="id", aggfunc="count"),
        return_count=pd.NamedAgg(column="returned_at", aggfunc=lambda x: x.notnull().sum()),
        unique_customers=pd.NamedAgg(column="user_id_order", aggfunc=lambda x: x.nunique())
    ).reset_index()

    order_kpis['return_rate'] = order_kpis['return_count'] / order_kpis['total_orders']
    order_kpis.drop(columns=['return_count'], inplace=True)

    print("\n✅ Order KPIs:")
    print(order_kpis.head())

    sys.exit(0)

except Exception as e:
    print(f"❌ KPI Calculation Failed: {e}")
    sys.exit(1)
