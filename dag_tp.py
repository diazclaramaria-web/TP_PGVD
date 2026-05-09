import datetime
from datetime import timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import get_current_context
import pandas as pd
import io
from dotenv import load_dotenv
import os

load_dotenv("/home/diazclaramaria/.env")

DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
BUCKET = os.getenv("BUCKET")
KEY_PATH = os.getenv("KEY_PATH")

def read_csv_from_gcs(bucket_name, file_name):
    from google.cloud import storage
    client = storage.Client.from_service_account_json(KEY_PATH)
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    content = blob.download_as_text()
    return pd.read_csv(io.StringIO(content))

def write_csv_to_gcs(df, bucket_name, file_name):
    from google.cloud import storage
    client = storage.Client.from_service_account_json(KEY_PATH)
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    blob.upload_from_string(df.to_csv(index=False), content_type="text/csv")

def filtrar_datos():
    context = get_current_context()
    execution_date = context["ds"]

    product_views = read_csv_from_gcs(BUCKET, "product_views.csv")
    ads_views = read_csv_from_gcs(BUCKET, "ads_views.csv")
    active = read_csv_from_gcs(BUCKET, "active_advertisers.csv")

    active_ids = active['advertiser_id'].unique()

    product_views['date'] = pd.to_datetime(product_views['date']).dt.date
    ads_views['date'] = pd.to_datetime(ads_views['date']).dt.date
    execution_date = pd.to_datetime(execution_date).date()

    product_views = product_views[
        (product_views['date'] == execution_date) &
        (product_views['advertiser_id'].isin(active_ids))
    ]
    ads_views = ads_views[
        (ads_views['date'] == execution_date) &
        (ads_views['advertiser_id'].isin(active_ids))
    ]

    write_csv_to_gcs(product_views, BUCKET, "tmp/product_views_filtered.csv")
    write_csv_to_gcs(ads_views, BUCKET, "tmp/ads_views_filtered.csv")

def top_ctr():
    ads_views = read_csv_from_gcs(BUCKET, "tmp/ads_views_filtered.csv")

    clicks = ads_views[ads_views["type"] == "click"]
    impressions = ads_views[ads_views["type"] == "impression"]

    clicks = clicks.groupby(["advertiser_id", "product_id"]).size().reset_index(name="clicks")
    impressions = impressions.groupby(["advertiser_id", "product_id"]).size().reset_index(name="impressions")

    ctr = clicks.merge(impressions, on=["advertiser_id", "product_id"], how="left")
    ctr["ctr"] = ctr["clicks"] / ctr["impressions"]

    top = ctr.sort_values("ctr", ascending=False).groupby("advertiser_id").head(20)
    write_csv_to_gcs(top, BUCKET, "tmp/top_ctr.csv")

def top_product():
    product_views = read_csv_from_gcs(BUCKET, "tmp/product_views_filtered.csv")

    top = (
        product_views
        .groupby(['advertiser_id', 'product_id'])
        .size()
        .reset_index(name='views')
        .sort_values('views', ascending=False)
        .groupby('advertiser_id')
        .head(20)
    )
    write_csv_to_gcs(top, BUCKET, "tmp/top_products.csv")

def db_writing():
    from sqlalchemy import create_engine
    context = get_current_context()
    execution_date = context["ds"]

    top_ctr = read_csv_from_gcs(BUCKET, "tmp/top_ctr.csv")
    top_products = read_csv_from_gcs(BUCKET, "tmp/top_products.csv")

    top_ctr["date"] = execution_date
    top_products["date"] = execution_date

    engine = create_engine(
        f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )

    top_ctr.to_sql("top_ctr", engine, if_exists="append", index=False)
    top_products.to_sql("top_products", engine, if_exists="append", index=False)

with DAG(
    dag_id="dag_tp_ads",
    default_args={
        'retries': 3,
        'retry_delay': timedelta(minutes=5),
    },
    start_date=datetime.datetime(2026, 4, 20),
    schedule="0 0 * * *",
    catchup=True
) as dag:
    filtrar = PythonOperator(task_id="FiltrarDatos", python_callable=filtrar_datos)
    top_ctr_task = PythonOperator(task_id="TopCTR", python_callable=top_ctr)
    top_product_task = PythonOperator(task_id="TopProduct", python_callable=top_product)
    db = PythonOperator(task_id="DBWriting", python_callable=db_writing)

    filtrar >> [top_ctr_task, top_product_task] >> db