"""Nightly build of the shop warehouse."""
from datetime import datetime

from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLCheckOperator, SQLExecuteQueryOperator

with DAG("shop_daily", start_date=datetime(2026, 10, 1), schedule="@daily", catchup=False) as dag:
    staging = [SQLExecuteQueryOperator(task_id=m, conn_id="warehouse", sql=f"models/staging/{m}.sql")
               for m in ("stg_orders", "stg_payments", "stg_refunds")]
    revenue = SQLExecuteQueryOperator(task_id="revenue_daily", conn_id="warehouse", sql="models/marts/revenue_daily.sql")
    ltv = SQLExecuteQueryOperator(task_id="customer_ltv", conn_id="warehouse", sql="models/marts/customer_ltv.sql")
    check = SQLCheckOperator(task_id="revenue_landed", conn_id="warehouse",
                             sql="select count(*) > 0 from mart.revenue_daily where order_date = '{{ ds }}'")
    staging >> revenue >> check
    staging >> ltv
