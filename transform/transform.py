import pandas as pd
import boto3
import os
import sys
from botocore.exceptions import ClientError

# === CONFIG ===
BUCKET_NAME = "lab6ecs"
S3_PREFIX = "raw"
LOCAL_DIR = "/tmp"

FILES = {
    "products": "products.csv",
    "orders": "orders_part1.csv",
    "order_items": "order_items_part1.csv"
}

def download_file(s3_client, s3_key, local_path):
    try:
        s3_client.download_file(BUCKET_NAME, s3_key, local_path)
        print(f"✅ Downloaded: {s3_key}")
    except ClientError as e:
        raise Exception(f"❌ Failed to download {s3_key}: {e}")

def transform():
    # Load files
    products = pd.read_csv(os.path.join(LOCAL_DIR, FILES['products']))
    orders = pd.read_csv(os.path.join(LOCAL_DIR, FILES['orders']))
    order_items = pd.read_csv(os.path.join(LOCAL_DIR, FILES['order_items']))

    # Merge
    df = order_items.merge(orders, on="order_id", suffixes=("_item", "_order"))
    df = df.merge(products, left_on="product_id", right_on="id")

    # Dates
    df['created_at_order'] = pd.to_datetime(df['created_at_order'], errors='coerce')
    df['order_date'] = df['created_at_order'].dt.date

    ### === CATEGORY KPIs ===
    cat_kpis = df.groupby(['category', 'order_date']).agg(
        daily_revenue=pd.NamedAgg(column="sale_price", aggfunc="sum"),
        avg_order_value=pd.NamedAgg(column="sale_price", aggfunc="mean"),
        total_returns=pd.NamedAgg(column="returned_at_item", aggfunc=lambda x: x.notnull().sum()),
        total_orders=pd.NamedAgg(column="order_id", aggfunc="count")
    ).reset_index()
    cat_kpis['avg_return_rate'] = cat_kpis['total_returns'] / cat_kpis['total_orders']
    cat_kpis.drop(columns=['total_returns', 'total_orders'], inplace=True)

    print("✅ Category KPIs:")
    print(cat_kpis.head())

    ### === ORDER KPIs ===
    order_kpis = df.groupby('order_date').agg(
        total_orders=pd.NamedAgg(column="order_id", aggfunc=lambda x: x.nunique()),
        total_revenue=pd.NamedAgg(column="sale_price", aggfunc="sum"),
        total_items_sold=pd.NamedAgg(column="id_x", aggfunc="count"),
        total_returns=pd.NamedAgg(column="returned_at_item", aggfunc=lambda x: x.notnull().sum()),
        unique_customers=pd.NamedAgg(column="user_id_order", aggfunc=lambda x: x.nunique())
    ).reset_index()
    order_kpis['return_rate'] = order_kpis['total_returns'] / order_kpis['total_orders']
    order_kpis.drop(columns=['total_returns'], inplace=True)

    print("\n✅ Order KPIs:")
    print(order_kpis.head())

def main():
    try:
        s3 = boto3.client("s3")
        for key in FILES.values():
            s3_path = f"{S3_PREFIX}/{key}"
            local_path = os.path.join(LOCAL_DIR, key)
            download_file(s3, s3_path, local_path)

        print("🚀 Starting KPI Transform...")
        transform()
        sys.exit(0)

    except Exception as e:
        print(f"❌ Transform Failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
