# /// script
# requires-python = ">=3.10"
# dependencies = ["sqlglot>=30", "pyyaml>=6"]
# ///
"""Self-check for datawright: builds a tiny repo and checks lineage, notes, staleness, savings and the MCP server. Run: uv run tests/test_datawright.py"""
import contextlib, importlib.util, io, json, os, sqlite3, subprocess, sys, tempfile
from pathlib import Path

spec = importlib.util.spec_from_file_location("datawright", Path(__file__).parents[1] / "skills/data-context/scripts/datawright.py")
datawright = importlib.util.module_from_spec(spec)
spec.loader.exec_module(datawright)

FILES = {
    "dbt_project.yml": "name: shop\n",
    "models/customers.sql": "select c.id, count(o.id) as orders\nfrom {{ ref('stg_customers') }} c\n"
                            "left join {{ ref('shop', 'stg_orders') }} o on o.customer_id = c.id\ngroup by 1\n",
    "macros/union_all.sql": "{% macro union_all(rels) %}\nselect * from mart.never_a_table\n{% endmacro %}\n",
    "models/heavy.sql": "{{ config(materialized='table') }}\n{% for x in [1, 2] %}\nselect * from {{ ref('customers') }} union all\n{% endfor %}\n",
    "hive/load_daily.hql": "insert overwrite table ${target_db}.web_daily select * from wmf.webrequest where dt = '${dt}';\n",
    "dags/dbt_run.py": "from airflow import DAG\nfrom airflow.operators.bash import BashOperator\n"
                       "with DAG('dbt_nightly', schedule='@daily') as dag:\n    BashOperator(task_id='run_dbt', bash_command='dbt run -s customers+')\n"
                       "    BashOperator(task_id='by_tag', bash_command=\"dbt build -s 'tag:finance+' --exclude heavy\")\n",
    "models/schema.yml": "version: 2\nmodels:\n  - name: customers\n    config:\n      tags: ['finance']\n"
                         "    columns:\n      - name: id\n        data_tests: [unique, not_null]\n",
    "contracts/revenue_daily.odcs.yaml": "apiVersion: v3.0.2\nkind: DataContract\nid: revenue-daily\nservers:\n  - server: prod\n"
                                         "    type: postgres\n    schema: mart\nschema:\n  - name: revenue_daily\n    properties:\n"
                                         "      - {name: order_date, logicalType: date, required: true, primaryKey: true}\n"
                                         "      - {name: revenue, logicalType: number, required: true}\n"
                                         "      - {name: refunds, logicalType: number, required: true}\n      - {name: note, logicalType: string}\n",
    "looker/revenue.view.lkml": "view: revenue {\n  sql_table_name: `mart.revenue_daily` ;;\n  dimension_group: order {\n    type: time\n"
                                "    timeframes: [date, week]\n    sql: ${TABLE}.order_date ;;\n  }\n  # comment: not a field\n"
                                "  dimension: revenue {\n    type: number\n    sql: ${TABLE}.revenue ;;\n  }\n"
                                "  measure: total_revenue { type: sum sql: ${revenue} ;; value_format: \"#,##0\" }\n}\n"
                                "view: top_days {\n  derived_table: {\n    sql: select order_date, revenue from ${revenue.SQL_TABLE_NAME}\n"
                                "      where {% condition region %} region {% endcondition %} ;;\n  }\n  dimension: order_date {}\n}\n",
    "looker/shop.model.lkml": "connection: \"warehouse\"\ninclude: \"*.view.lkml\"\nexplore: revenue {\n"
                              "  join: top_days { sql_on: ${revenue.order_date} = ${top_days.order_date} ;; relationship: one_to_one }\n}\n",
    "looker/sales.dashboard.lookml": "- dashboard: sales\n  title: Sales\n  elements:\n  - name: revenue_tile\n    model: shop\n"
                                     "    explore: revenue\n    type: single_value\n    fields: [revenue.total_revenue]\n",
    ".data-context/imports/metabase/metabase.json": json.dumps({
        "tables": [{"id": 1, "db": "wh", "schema": "mart", "name": "revenue_daily", "fields": [{"id": 10, "name": "order_date"}, {"id": 11, "name": "revenue"}]}],
        "cards": [{"id": 1, "name": "Revenue by day", "type": "question", "columns": ["order_date", "sum"],
                   "dataset_query": {"type": "query", "query": {"source-table": 1, "aggregation": [["sum", ["field", 11, None]]],
                                                                 "breakout": [["field", 10, {"temporal-unit": "day"}]]}}},
                  {"id": 2, "name": "Revenue (SQL)", "type": "model", "columns": ["order_date", "revenue"],
                   "dataset_query": {"type": "native", "native": {"query": "select order_date, revenue from warehouse.mart.revenue_daily where 1=1 [[and order_date > {{start}}]]"}}},
                  {"id": 3, "name": "Weekly revenue", "type": "question", "columns": ["sum"],
                   "dataset_query": {"lib/type": "mbql/query", "stages": [{"lib/type": "mbql.stage/mbql", "source-card": 2,
                                                                           "aggregation": [["sum", {}, ["field", {}, 11]]]}]}},
                  {"id": 5, "name": "Revenue table", "type": "question", "dataset_query": {"lib/type": "mbql/query", "stages": [{"source-card": 2}]}},
                  {"id": 4, "name": "Old", "archived": True, "dataset_query": {"type": "query", "query": {"source-table": 1}}}],
        "dashboards": [{"id": 1, "name": "Exec KPIs", "cards": [1, 3, 5]}]}),
    # a lakehouse: jobs that meet only through files on S3, OSS and HDFS
    "lake/ingest.py": 'df = spark.read.json("s3a://raw-bucket/events/dt=2026-11-04/*.json")\ndf.write.mode("overwrite").parquet("s3://lake/bronze/events/")\n',
    "lake/bronze.sql": "create external table bronze.events (id bigint) stored as parquet location 's3://lake/bronze/events';\n",
    "lake/silver.py": 'spark.table("bronze.events").filter("id > 0").writeTo("lake.silver.events").append()\n',
    "lake/doris_load.sql": 'insert into ods.clicks select * from S3("uri" = "s3://raw-bucket/clicks/2026/11/04/part-0.parquet", "format" = "parquet");\n',
    "lake/export.sql": "insert overwrite directory 'oss://exports/daily_revenue' select * from staging.payments;\n",
    "lake/flink_sink.sql": "create table files_sink (id bigint) with ('connector' = 'filesystem', 'path' = 's3a://lake/flink/orders', 'format' = 'parquet');\n"
                           "insert into files_sink select id from staging.orders;\n",
    "lake/pandas_job.py": 'import pandas as pd\nfor table in TABLES:\n    pd.read_parquet(f"s3://lake/{table}/data")\npd.read_csv("s3://raw-bucket/ref/countries.csv")\n',
    "lake/payments.py": "import os\nfor table in TABLES:\n    spark.read.parquet(f\"s3://lake/raw/{table}/\").write.saveAsTable(f\"raw.{table}\")\n"
                        "spark.table(\"staging.payments\").write.parquet(f\"s3a://{os.environ['LAKE_BUCKET']}/staging/payments/{run_date}\")\n",
    "lake/payments_raw.sql": "create external table staging.payments_raw (id int) location 's3://prod-lake/staging/payments';\n",
    "lake/dags/s3_to_iceberg.py": 'from airflow import DAG\nfrom airflow.operators.bash import BashOperator\nscript = "/opt/airflow/lake/jobs/kafka_to_iceberg.py"\nwith DAG("s3_to_iceberg", schedule="5 * * * *") as dag:\n    BashOperator(task_id="load_orders", bash_command=f"""\n        spark-submit --master spark://m:7077 \\\\\n            {script} \\\\\n            orders "{{{{ data_interval_start }}}}"\n        """)\n    SparkSubmitOperator(task_id="load_payments", application="lake/jobs/kafka_to_iceberg.py", application_args=["payments", "{{ ds }}"])\n',
    "lake/jobs/kafka_to_iceberg.py": 'import os, sys\n\ndef load(table_name, start):\n    target = f"iceberg_catalog.default.{table_name}_raw"\n    date_str, hour_str = start[:10], start[11:13]\n    path = f"s3a://{os.environ[\'S3_BUCKET\']}/data/kafka/{table_name}/{date_str}/{hour_str}/"\n    spark.read.json(path).writeTo(target).overwritePartitions()\n\nif __name__ == "__main__":\n    tbl = sys.argv[1]\n    load(tbl, sys.argv[2])\n',
    "lake/jobs/to_silver.py": 'import argparse\np = argparse.ArgumentParser()\np.add_argument("source")\np.add_argument("--target-table", dest="target")\np.add_argument("--catalog", default="lakehouse")\nargs = p.parse_args()\nspark.table(f"raw.{args.source}").writeTo(f"{args.catalog}.silver.{args.target}").append()\n',
    "lake/dags/silver.py": 'from airflow import DAG\nfrom airflow.operators.bash import BashOperator\nwith DAG("silver", schedule="@daily") as dag:\n    BashOperator(task_id="clicks", bash_command="python lake/jobs/to_silver.py clicks --target-table=clicks_clean")\n    BashOperator(task_id="sessions", bash_command="python lake/jobs/to_silver.py sessions --target-table sessions_clean")\n    BashOperator(task_id="gold_orders", bash_command="python lake/jobs/click_job.py orders -t gold.orders")\n',
    "lake/jobs/copy_many.py": 'def copy(src, dst):\n    spark.table(src).write.saveAsTable(dst)\n\ncopy("raw.a", "stg.a")\ncopy("raw.b", "stg.b")\nfor t in ["c", "d"]:\n    spark.table(f"raw.{t}").write.saveAsTable(f"stg.{t}")\n',
    "lake/config/tables.yaml": "tables:\n  - {name: invoices, schema: finance}\n  - {name: refunds, schema: finance}\n",
    "lake/dags/factory.py": 'import yaml\nfrom pathlib import Path\nfrom airflow import DAG\nfrom airflow.operators.python import PythonOperator\nconf = yaml.safe_load(open(Path(__file__).parent.parent / "config" / "tables.yaml"))\nwith DAG("factory", schedule="@daily") as dag:\n    for t in conf["tables"]:\n        PythonOperator(task_id=f"load_{t[\'name\']}", python_callable=print)\n        spark.table(f"{t[\'schema\']}.{t[\'name\']}_raw").write.saveAsTable(f"{t.get(\'schema\')}.{t[\'name\']}")\n',
    "lake/jobs/click_job.py": 'import click\n\n@click.command()\n@click.argument("source")\n@click.option("--target", "-t", default="gold.default_out")\ndef main(source, target):\n    spark.table(f"silver.{source}").write.saveAsTable(target)\n',
    "lake/jobs/common.py": 'def publish(table):\n    spark.table(f"stage.{table}").write.saveAsTable(f"pub.{table}")\n',
    "lake/jobs/publish_all.py": 'from lake.jobs.common import publish\n\npublish("users")\npublish(table="teams")\n',
    "lake/jobs/entry.py": "from lake.jobs.middle import run\n\nrun(\"cart\")\nrun(\"wishlist\")\n",
    "lake/jobs/middle.py": "from lake.jobs.sink import write\n\ndef run(name):\n    write(f\"app_{name}\")\n",
    "lake/jobs/sink.py": "from lake.jobs.middle import run  # a cycle back: must not loop\n\ndef write(t):\n    spark.table(f\"src.{t}\").write.saveAsTable(f\"mart.{t}\")\n",
    'practices/dags/load.py': "from airflow import DAG\nfrom airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator\nwith DAG('load', schedule='0 2 * * *') as dag:\n    SQLExecuteQueryOperator(task_id='events', sql='practices/sql/events.sql')\n    SQLExecuteQueryOperator(task_id='users', sql='practices/sql/users.sql')\n    SQLExecuteQueryOperator(task_id='shared', sql='practices/sql/shared_a.sql')\n",
    'practices/dags/report.py': "from airflow import DAG\nfrom airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator\nwith DAG('report', schedule='0 1 * * *') as dag:\n    SQLExecuteQueryOperator(task_id='daily', sql='practices/sql/daily.sql')\n    SQLExecuteQueryOperator(task_id='shared', sql='practices/sql/shared_b.sql')\n    SQLExecuteQueryOperator(task_id='feedback', sql='practices/sql/feedback.sql')\n",
    'practices/sql/events.sql': 'insert into dw.events select * from ods.events;\n',
    'practices/sql/users.sql': 'create table dw.users (id int, name varchar, primary key (id));\ninsert into dw.users select id, name from ods.users;\n',
    'practices/sql/shared_a.sql': 'insert overwrite table dw.shared select id from ods.a;\n',
    'practices/sql/shared_b.sql': 'insert overwrite table dw.shared select id from ods.b;\n',
    'practices/sql/daily.sql': 'insert overwrite table ads.daily select count(id) as n from dw.events;\n',
    'practices/sql/feedback.sql': 'insert overwrite table ods.feedback select n from ads.daily;\n',
    'practices/sql/cycle.sql': 'insert overwrite table dw.loop_a select id from dw.loop_b;\ninsert overwrite table dw.loop_b select id from dw.loop_a;\n',
    'practices/conn.py': '# Example: postgresql://user:example123@host/db\nURL = "postgresql://etl:Sup3rS3cret@db:5432/wh"\npassword = "changeme"\n',
    'models/inc_orders.sql': "{{ config(materialized='incremental') }}\nselect id, amount from {{ ref('stg_orders') }}\n",
    "models/fct_events.sql": "{{ config(materialized='table', partition_by={'field': 'event_date', 'data_type': 'date'}, cluster_by=['kind', 'user_id']) }}\n"
                             "select event_date, kind, user_id from {{ ref('stg_orders') }}\n",
    "models/fct_daily.sql": "select * from {{ ref('fct_events') }} where event_date >= current_date - 7 and kind = 'x'\n",
    "models/fct_weekly.sql": "select * from {{ ref('fct_events') }} where event_date >= current_date - 30 and kind = 'y'\n",
    "concepts/ddl.sql": "create table fin.wallet (player_id bigint, amount decimal(10, 2));\ncreate table fin.bets (player_id bigint, amount decimal(10, 2));\n"
                        "create table app.sessions (session_id bigint, playerid varchar(36), device_ref bigint, started timestamp);\n",
    'service/src/main/resources/mapper/PlayerMapper.xml': '<?xml version="1.0" encoding="UTF-8"?>\n<!DOCTYPE mapper PUBLIC "-//mybatis.org//DTD Mapper 3.0//EN" "http://mybatis.org/dtd/mybatis-3-mapper.dtd">\n<mapper namespace="com.x.PlayerMapper">\n  <sql id="cols">p.id, p.name</sql>\n  <select id="find" resultType="map">\n    -- players with their wallets\n    SELECT <include refid="cols"/>, w.amount FROM crm.player_accounts p JOIN fin.wallet w ON w.player_id = p.id\n    <where>\n      <if test="a != null">AND p.id = #{a}</if>\n      <choose><when test="t == 1">AND w.amount &gt; 0</when><otherwise>AND p.id IN (SELECT player_id FROM fin.bets)</otherwise></choose>\n    </where>\n  </select>\n  <select id="byTable">SELECT * FROM ${table} WHERE id = #{id}</select>\n</mapper>\n',
    'meaning/ddl.sql': 'create table fin.requests (id bigint, type int, status int, amount_cents bigint, created_ms bigint, created_at timestamp);\n',
    'meaning/report.sql': "insert overwrite table rep.requests select r.id, case r.status when 1 then 'pending' when 2 then 'approved' when 3 then 'rejected' end as status_label,\nsum(case when r.type = 1 then r.amount_cents end) as deposit_amount, r.amount_cents / 100 as amount_usd,\nfrom_unixtime(r.created_ms / 1000) as created, convert_tz(r.created_at, 'UTC', 'Asia/Manila') as created_local\nfrom fin.requests r group by 1, 2, 4, 5, 6;\n",
    'meaning/other.sql': "insert overwrite table rep.requests_b select decode(status, 1, 'pending', 2, 'approved', 3, 'declined') as s from fin.requests;\n",
    'meaning/third.sql': "insert overwrite table rep.requests_c select case when status = 1 then 'pending' when status in (2) then 'approved' when status = 3 then 'rejected' end as s from fin.requests;\n",
    "models/semantic.yml": "semantic_models:\n  - name: events_sm\n    model: ref('fct_events')\n    entities:\n      - {name: account, type: foreign, expr: user_id}\n"
                           "  - name: customers_sm\n    model: ref('customers')\n    entities:\n      - {name: account, type: primary, expr: id}\n",
    "macros/shared_dims.sql": "{% macro shared_dims() %}\nselect * from {{ ref('customers') }} join ({{ inner_dims() }}) d using (id)\n{% endmacro %}\n"
                              "{# {{ ref('never_read') }} #}\n{% macro inner_dims() %}select * from {{ source('crm_src', 'segments') }}{% endmacro %}\n",
    "models/via_macro.sql": "with latest as ({{ dedupe(relation = source('raw_src', 'events'), by = 'id') }})\nselect * from latest join ({{ shared_dims() }}) s using (id)\n",
    'concepts/accounts.sql': 'create table crm.player_accounts (id bigint, name varchar, primary key (id));\n',
    'concepts/wallet.sql': 'insert overwrite table bi.wallet select w.player_id, p.name from fin.wallet w join crm.player_accounts p on w.player_id = p.id;\n',
    'concepts/bets.sql': 'insert overwrite table bi.bets select b.player_id, b.amount from fin.bets b join crm.player_accounts p on b.player_id = p.id;\n',
    'concepts/sessions.sql': 'insert overwrite table bi.sessions select s.playerid as uid, s.started from app.sessions s join crm.player_accounts p on p.id = s.playerid;\n',
    'concepts/rollup.sql': 'insert overwrite table bi.rollup select uid as member_ref, count(*) as n from bi.sessions group by uid;\n',
    'advise/ddl.sql': 'create table wh.events (event_id int, user_id int, event_date date, kind varchar) partitioned by (event_id);\n',
    'advise/daily.sql': "insert overwrite table rep.kinds select e.kind, count(*) as n from wh.events e join wh.users u on u.user_id = e.user_id\nwhere e.event_date >= '2026-01-01' and e.kind = 'click' group by e.kind;\n",
    'advise/today.sql': "insert overwrite table rep.today select e.event_id from wh.events e join wh.users u on e.user_id = u.user_id\nwhere e.event_date = current_date and e.kind in ('view');\n",
    "lake/Agg.java": 'class Agg { void run() { spark.read().parquet("s3a://lake/bronze/events")\n  .groupBy("id").count().write().mode("append").save("hdfs://nn/warehouse/agg"); } }\n',
    "models/customer_orders.sql": "with stg_orders as (select * from {{ ref('stg_orders') }})\nselect count(*) as n from stg_orders\n",
    "pipelines/revenue.sql": "-- daily revenue\ninsert into mart.revenue_daily\nselect o.order_date, sum(p.amount) as revenue\n"
                             "from staging.orders o join staging.payments p on p.order_id = o.id\n"
                             "where o.order_date = '{{ ds }}'\ngroup by 1;\n",
    "dags/load_orders.py": 'SQL = """\nINSERT INTO staging.orders SELECT * FROM raw.orders WHERE dt = \'{{ ds }}\'\n"""\n'
                           'spark.table("raw.events").write.saveAsTable("staging.events")\n',
    "reports/weekly.py": 'q = "select sum(revenue) from mart.revenue_daily where order_date >= current_date - 7"\n',
    "broken.sql": "select from where;\n",
    "dags/factory.py": 'STAGING = "staging.events_stage"\nSRC = "raw.clicks"\n\ndef build():\n'
                       '    """Select the latest rows and load them."""\n'
                       '    a = f"INSERT INTO {STAGING} SELECT * FROM raw.events"\n'
                       '    b = "INSERT INTO mart.clicks_daily " + "SELECT day, count(*) AS n FROM " + SRC + " GROUP BY day"\n'
                       '    c = "insert into {t} select * from raw.x".format(t="dw.x")\n'
                       '    d = "SELECT id FROM players WHERE updated_at > %s"\n'
                       '    e = "insert into {{ params.db }}.orders_copy select * from staging.orders"\n'
                       '    return a, b, c, d, e\n',
    "spark/jdbc_job.py": 'src = spark.read.format("jdbc").option("dbtable", "src.jdbc_table").load()\n'
                         'src.write.format("jdbc").option("dbtable", "dw.jdbc_out").save()\n',
    "dags/backfill_revenue.py": 'SQL = "insert into mart.revenue_daily select order_date, 0 as revenue from staging.orders"\n',
    "tests/test_revenue.py": 'FIXTURE = "create table staging.orders as select 1 as id, current_date as order_date"\n',
    "admin/users.sql": "create user if not exists app identified by 'pw-secret-123';\n",
    "flink/from_kafka.sql": "CREATE TABLE src (a INT) WITH ('connector' = 'kafka', 'topic' = 't1',\n"
                            "  'properties.sasl.jaas.config' = 'PlainLoginModule required password=\"pw-secret-456\";');\n"
                            "INSERT INTO mart.from_kafka SELECT a FROM src;\nselect 'unterminated pw-secret-789 from t\n",
    "ddl/mart.sql": "create table if not exists mart.revenue_daily (order_date date, revenue decimal(18, 2));\n",
    "dags/etl.py": "from datetime import datetime, timedelta\nfrom airflow import DAG\nfrom airflow.decorators import dag, task\n\n"
                   'MARTS = ["revenue"]\n'
                   'etl = DAG(dag_id="etl", schedule=timedelta(hours=6), template_searchpath=["/opt/airflow/pipelines"])\n'
                   "for m in MARTS:\n"
                   '    SQLExecuteQueryOperator(task_id=f"build_{m}", sql=f"{m}.sql", dag=etl)\n'
                   'SparkSubmitOperator(task_id="jdbc_copy", application="/opt/airflow/spark/jdbc_job.py", dag=etl)\n\n'
                   "@dag(schedule=None, start_date=datetime(2026, 1, 1))\ndef adhoc():\n    @task\n    def snapshot():\n"
                   '        run_sql("insert into mart.revenue_snapshot select * from mart.revenue_daily")\n    snapshot()\n',
    # catalog exports: what reads mart.revenue_daily outside the repo, in each tool's own format
    ".data-context/imports/openlineage.jsonl": json.dumps({"eventType": "COMPLETE", "job": {"namespace": "looker", "name": "revenue"},
        "inputs": [{"namespace": "pg", "name": "mart.revenue_daily"}],
        "outputs": [{"namespace": "looker", "name": "bi.revenue_dashboard", "facets": {
            "schema": {"fields": [{"name": "revenue", "type": "DECIMAL"}]},
            "columnLineage": {"fields": {"revenue": {"inputFields": [{"namespace": "pg", "name": "mart.revenue_daily", "field": "revenue"}]}}}}}]}) + "\n",
    ".data-context/imports/datahub.json": json.dumps([
        {"entityUrn": "urn:li:dataset:(urn:li:dataPlatform:postgres,reporting.finance_extract,PROD)", "aspectName": "upstreamLineage",
         "aspect": {"json": {"upstreams": [{"dataset": "urn:li:dataset:(urn:li:dataPlatform:postgres,mart.revenue_daily,PROD)"}],
                             "fineGrainedLineages": [{"upstreams": ["urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:postgres,mart.revenue_daily,PROD),revenue)"],
                                                      "downstreams": ["urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:postgres,reporting.finance_extract,PROD),amount)"]}]}}},
        {"entityUrn": "urn:li:dataset:(urn:li:dataPlatform:postgres,reporting.finance_extract,PROD)", "aspectName": "schemaMetadata",
         "aspect": {"value": json.dumps({"fields": [{"fieldPath": "amount", "nativeDataType": "numeric"}]})}}]),
    ".data-context/imports/openmetadata_lineage.json": json.dumps({"entity": {"id": "e1", "fullyQualifiedName": "warehouse.mart.revenue_daily"},
        "nodes": [{"id": "n1", "fullyQualifiedName": "warehouse.ml.revenue_forecast"}], "upstreamEdges": [],
        "downstreamEdges": [{"fromEntity": "e1", "toEntity": "n1", "lineageDetails": {"columnsLineage": [
            {"fromColumns": ["warehouse.mart.revenue_daily.revenue"], "toColumn": "warehouse.ml.revenue_forecast.forecast"}]}}]}),
    ".data-context/imports/openmetadata_tables.json": json.dumps({"data": [{"fullyQualifiedName": "warehouse.ml.revenue_forecast",
        "description": "Next 30 days of revenue", "columns": [{"name": "forecast", "dataType": "DECIMAL"}]}]}),
    # connectors: hand-written lineage, Kafka Connect configs, and one connector that breaks
    "lineage/bi_exposures.yaml": "edges:\n  - from: {type: table, fqn: svc.mart.revenue_daily}\n    to: {type: dashboard, fqn: svc.looker.revenue_board}\n"
                                 "    description: finance board\n",
    "connect/orders-cdc.json": json.dumps({"name": "orders-cdc", "config": {"connector.class": "io.debezium.connector.mysql.MySqlConnector",
                                          "topic.prefix": "shopdb", "table.include.list": "shop.orders"}}),
    "connect/orders-sink.json": json.dumps({"name": "orders-sink", "config": {"connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
                                           "topics": "shopdb.shop.orders", "table.name.format": "raw.orders_cdc"}}),
    "configs/orders_job.yaml": "dag_id: orders_hourly\nschedule: '@hourly'\nsteps:\n  - sql: insert into mart.orders_h select * from raw.orders\n",
    "assets/dagster_assets.py": "import dagster as dg\n\n@dg.asset(key_prefix=['analytics'], deps=[dg.AssetKey(['raw', 'orders'])])\n"
                                "def daily_orders():\n    return 1\n\n@dg.asset\ndef order_report(daily_orders):\n    return 2\n",
    "assets/jobs.py": "import dagster as dg\norders_job = dg.define_asset_job('orders_job', selection=[dg.AssetKey(['analytics', 'daily_orders'])])\n",
    "assets/schedules.py": "import dagster as dg\nfrom .jobs import orders_job\nnightly = dg.ScheduleDefinition(job=orders_job, cron_schedule='0 2 * * *')\n",
    # an in-house framework: jobs/<table>/query.sql is a SELECT into its folder's table; job.yaml schedules it
    "jobs/mart.daily_kpis/query.sql": "select order_date, count(*) as orders from staging.orders group by 1\n",
    "jobs/mart.daily_kpis/job.yaml": "dag: kpis\nschedule: '@daily'\ndescription: KPIs per day\n",
    ".data-context/connectors/framework.py": (
        "OWNS = True\nROOT = None\n\ndef match(path):\n    return path.startswith('jobs/')\n\n"
        "def extract(path, text):\n    import yaml\n    table = path.split('/')[1]\n"
        "    if path.endswith('.sql'):\n        yield {'sql': f'insert into {table} ' + text}\n"
        "    else:\n        job = yaml.safe_load(text)\n        assert (ROOT / path).exists()\n"
        "        yield {'dataset': table, 'description': job['description']}\n"
        "        yield {'run': {'dag': job['dag'], 'task': table, 'schedule': job['schedule'], 'file': path.replace('job.yaml', 'query.sql')}}\n"),
    ".data-context/connectors/broken.py": "def match(path):\n    return path.endswith('.yaml')\n\ndef extract(path, text):\n"
                                          "    raise ValueError('cannot read ' + repr(text))\n",
    ".data-context/notes/mart.revenue_daily.yml": "dataset: mart.revenue_daily\nnotes:\n"
                                                  "- {date: 2020-01-01, kind: gotcha, text: Refunds land three days late}\n",
}


def cli(*args):
    out = io.StringIO()
    with contextlib.redirect_stdout(out):
        datawright.main(list(args))
    return out.getvalue()


def main():
    assert datawright.strip_jinja("{{ ref('pkg', 'm') }} {{ source('s', 't') }} {% if x %}a{% endif %}") == "m s.t  a "
    # dbt selectors, the way dbt reads them: union across tokens, intersection within commas, + and n+ for graph hops
    assert datawright.dbt_args("build -s 'tag:a+ stg_x' --exclude tag:b --target dev") == (["tag:a+", "stg_x"], ["tag:b"])
    models = {"m1": "models/m1.sql", "m2": "models/sub/m2.sql", "m3": "models/m3.sql"}
    names, tags = {**models, "src.t": None}, {"src.t": {"daily"}, "m3": {"daily"}}
    ups, downs = {"m1": {"src.t"}, "m2": {"m1"}}, {"src.t": {"m1"}, "m1": {"m2"}}
    sel = lambda *tok: datawright.dbt_select(list(tok), models, names, tags, ups, downs)
    assert sel("tag:daily+") == {"src.t", "m1", "m2", "m3"} and sel("+m2") == {"src.t", "m1", "m2"} and sel("m1+1") == {"m1", "m2"}
    assert sel("path:models/sub") == {"m2"} and sel("source:src+") == {"src.t", "m1", "m2"} and sel("tag:daily,m3") == {"m3"}
    assert sel("m*") == set(models) and sel("state:modified+") is None, "state needs dbt's run state"
    resolve = datawright.resolver({"orders", "shop.raw.orders", "raw.orders"}, {})
    assert resolve("orders") == "shop.raw.orders" and resolve("raw.orders") == "shop.raw.orders"

    hints = Path(tempfile.mkdtemp())  # the engine a repo's own config names: Doris images and connectors, MaxCompute SDKs
    (hints / "docker-compose.yml").write_text("services:\n  fe:\n    image: apache/doris:2.1.7-fe\n")
    (hints / "requirements.txt").write_text("pyodps==0.12\n")
    datawright.ROOT = hints
    assert datawright.dialect_hint(["docker-compose.yml"]) == "doris" and datawright.dialect_hint(["requirements.txt"]) == "hive"
    repo = Path(tempfile.mkdtemp()) / "repo"  # its own parent, so the path-escape check can't see another run's file
    for c in ("lineage_yaml.py", "kafka_connect.py", "dagster.py", "odcs.py", "lookml.py", "superset.py", "metabase.py", "mybatis.py"):
        FILES[f".data-context/connectors/{c}"] = (Path(__file__).parents[1] / "skills/data-context/assets/connectors" / c).read_text()
    FILES[".data-context/connectors/lineage_yaml.py"] = FILES[".data-context/connectors/lineage_yaml.py"].replace("DROP_PARTS = 0", "DROP_PARTS = 1")
    for path, text in FILES.items():
        (repo / path).parent.mkdir(parents=True, exist_ok=True)
        (repo / path).write_text(text)
    import zipfile  # a Superset export as downloaded from the UI, with one member trying to escape the folder
    with zipfile.ZipFile(repo / ".data-context/imports/superset_finance.zip", "w") as z:
        z.writestr("export/datasets/wh/revenue_daily.yaml", "table_name: revenue_daily\nschema: mart\nsql: null\nuuid: d1\ndatabase_uuid: db1\n"
                   "columns:\n- {column_name: order_date, type: DATE}\n- {column_name: revenue, type: DECIMAL}\n"
                   "metrics:\n- {metric_name: total_revenue, expression: SUM(revenue)}\n")
        z.writestr("export/charts/Revenue_trend_1.yaml", "slice_name: Revenue trend\nviz_type: echarts_timeseries\nuuid: c1\ndataset_uuid: d1\n"
                   "params: {x_axis: order_date, metrics: [total_revenue], adhoc_filters: []}\n")
        z.writestr("export/dashboards/Finance_1.yaml", "dashboard_title: Finance\nuuid: b1\nposition:\n  CHART-x: {type: CHART, meta: {uuid: c1}}\n")
        z.writestr("../../escaped.txt", "x")
    git = lambda *a: subprocess.run(["git", "-c", "user.email=t@t", "-c", "user.name=t", *a], cwd=repo, check=True, capture_output=True)
    git("init", "-q")
    git("add", "-A")
    git("commit", "-qm", "init")
    os.chdir(repo)

    err = io.StringIO()
    with contextlib.redirect_stderr(err):
        built = cli("build")
    print(built)
    assert "auto-detected" in built, built
    assert "pw-secret-123" not in built + err.getvalue(), "SQL text (and secrets in it) must never be echoed"
    con = sqlite3.connect(repo / ".data-context/index.db")
    edges = set(con.execute("select src, dst from edges"))
    for e in [("staging.orders", "mart.revenue_daily"), ("staging.payments", "mart.revenue_daily"),
              ("raw.orders", "staging.orders"), ("stg_customers", "customers"), ("stg_orders", "customers"),
              ("raw.events", "staging.events"), ("raw.events", "staging.events_stage"), ("raw.clicks", "mart.clicks_daily"),
              ("raw.x", "dw.x"), ("staging.orders", "orders_copy"), ("src.jdbc_table", "dw.jdbc_out"),
              ("stg_orders", "customer_orders"),  # a CTE named after the model it reads (common dbt style)
              ("customers", "heavy"),  # ref() in a model whose Jinja keeps it from parsing
              ("wmf.webrequest", "web_daily")]:  # ${vars} in Hive SQL
        assert e in edges, f"missing edge {e}: {edges}"
    assert ("staging.payments.amount", "mart.revenue_daily.revenue") in set(con.execute("select * from col_edges"))
    assert con.execute("select line from readers where dataset='mart.revenue_daily' and file='reports/weekly.py'").fetchone() == (1,)
    assert con.execute("select file, line from datasets where name='mart.revenue_daily'").fetchone() == ("pipelines/revenue.sql", 2)
    assert con.execute("select stale from notes").fetchone()[0].startswith("code changed")
    assert con.execute("select count(*) from issues where file='broken.sql'").fetchone()[0] == 1
    assert con.execute("select count(*) from issues where file='dags/factory.py'").fetchone()[0] == 0, "docstrings are not SQL"
    assert ("players", "dags/factory.py") in set(con.execute("select dataset, file from readers"))
    assert not con.execute("select 1 from datasets where name = 'mart.never_a_table'").fetchone(), "dbt macros are templates, not queries"
    runs_ = set(con.execute("select file, dag, task from runs"))
    assert ("models/customers.sql", "dbt_nightly", "run_dbt") in runs_, "dbt run -s links the model"
    assert ("models/customers.sql", "dbt_nightly", "by_tag") in runs_ and ("models/heavy.sql", "dbt_nightly", "by_tag") not in runs_, "tags from schema YAML, --exclude"
    assert ("customers", "finance") in set(con.execute("select * from tags"))
    assert ("staging.orders", "mart.daily_kpis") in edges and not con.execute("select 1 from readers where file like 'jobs/%'").fetchone(), "OWNS replaces the built-in read"
    assert ("jobs/mart.daily_kpis/query.sql", "kpis", "mart.daily_kpis", "@daily") in set(con.execute("select file, dag, task, schedule from runs"))
    assert con.execute("select description from datasets where name='mart.daily_kpis'").fetchone()[0] == "KPIs per day"
    for e in [("raw.orders", "analytics.daily_orders"), ("analytics.daily_orders", "order_report")]:  # Dagster deps, AssetKey and parameters
        assert e in edges, f"dagster edge missing {e}"
    assert "runs in (1): orders_job.analytics.daily_orders 0 2 * * * (assets/schedules.py:3)" in cli("card", "analytics.daily_orders"), "a schedule in another file"
    for e in [("mart.revenue_daily", "looker.revenue_board"), ("shop.orders", "kafka:shopdb.shop.orders"), ("kafka:shopdb.shop.orders", "raw.orders_cdc")]:
        assert e in edges, f"connector edge missing {e}"
    broken = [r[0] for r in con.execute("select error from issues where error like 'connector broken%'")]
    assert broken and not any("finance board" in b or "revenue_daily" in b for b in broken), f"a broken connector is reported, never quoted: {broken}"
    assert ("staging.orders", "tests/test_revenue.py") in set(con.execute("select dataset, file from readers"))
    assert not con.execute("select 1 from writers where file like 'tests/%'").fetchone(), "test fixtures don't produce data"
    assert "pw-secret-123" not in str(con.execute("select * from issues").fetchall())
    assert ("kafka:t1", "mart.from_kafka") in edges, "a ';' inside a string must not split the statement"
    leak = [(s, w) for s in ("pw-secret-456", "pw-secret-789") for w, v in (("issues", str(con.execute("select * from issues").fetchall())), ("stdout", built), ("stderr", err.getvalue())) if s in v]; assert not leak, leak
    assert con.execute("select count(*) from issues where file='flink/from_kafka.sql'").fetchone()[0] == 1, "the unterminated string is reported"
    runs = set(con.execute("select file, dag, task, schedule from runs"))
    for r in [("pipelines/revenue.sql", "etl", "build_revenue", "timedelta(hours=6)"),  # loop + template_searchpath
              ("spark/jdbc_job.py", "etl", "jdbc_copy", "timedelta(hours=6)"), ("dags/etl.py", "adhoc", "snapshot", "manual")]:
        assert r in runs, f"missing Airflow run {r}: {runs}"
    assert "runs in (1): adhoc.snapshot manual (dags/etl.py:14)" in cli("card", "revenue_snapshot")

    cov = cli("coverage")
    for text in ("connector lineage_yaml", "connector kafka_connect", "1 look like pipeline or DAG definitions in config", "configs/orders_job.yaml",
                 "connector dagster", "next: write a connector"):
        assert text in cov, f"coverage is missing {text!r}:\n{cov}"
    assert "connector broken" not in cov and "Dagster (" not in cov, "a failed connector reads nothing; a tool with a connector isn't listed as unread"
    card = cli("card", "revenue_daily")
    for text in ("produced by pipelines/revenue.sql:2", "schema in ddl/mart.sql:1", "also written by (1): dags/backfill_revenue.py:1",
                 "revenue DECIMAL(18, 2)", "same names from staging.orders (1)", "staging.orders", "reports/weekly.py:1 (reads)", "STALE", "revenue ← staging.payments.amount"):
        assert text in card, f"card is missing {text!r}:\n{card}"
    assert "mart.revenue_daily" in cli("lineage", "raw.orders", "--down", "9", "--up", "0")
    assert "staging.payments.amount" in cli("lineage", "mart.revenue_daily.revenue", "--up", "2", "--down", "0")
    assert cli("search", "revenue").splitlines()[0].startswith("mart.revenue_daily")
    (repo.parent / "outside.sql").write_text("select 'outside the repo'")  # as a crafted dbt manifest could point
    con.execute("insert into datasets (name, kind, file, line) values ('x.escape', 'table', '../outside.sql', 1)")
    con.commit()
    with contextlib.suppress(SystemExit):
        assert "outside the repo" not in cli("code", "x.escape"), "code must only read files inside the repo"

    cli("note", "revenue_daily", "Excludes test orders", "--kind", "correction", "--column", "revenue", "--source", "CMS-1234")
    cli("set", "revenue_daily", "grain=one row per order_date", "key=order_date", "meaning=daily revenue")
    card = cli("card", "mart.revenue_daily")
    assert "readiness 60/100 · missing: owner, tests, no stale notes" in card, card  # grain, key, schedule, producer, verified
    assert "contract (contracts/revenue_daily.odcs.yaml): 4 columns, 3 required" in card
    assert "CONTRACT BROKEN: required column refunds is no longer produced" in card and "revenue_daily: required column refunds" in cli("lint")
    # object storage: paths are datasets, partitions and files fold into one name, and a table stored at a path is that path
    with sqlite3.connect(repo / ".data-context/index.db") as c:
        edges = {(s, d) for s, d in c.execute("select src, dst from edges")}
        names = {n for (n,) in c.execute("select name from datasets")}
    for e in [("s3://raw-bucket/events", "bronze.events"), ("bronze.events", "lake.silver.events"), ("s3://raw-bucket/clicks", "ods.clicks"),
              ("staging.payments", "oss://exports/daily_revenue"), ("staging.orders", "s3://lake/flink/orders"), ("bronze.events", "hdfs://nn/warehouse/agg"),
              ("staging.payments", "staging.payments_raw"),
              ("s3://{bucket}/data/kafka/orders", "iceberg_catalog.default.orders_raw"),  # one script, its table from each task's arguments
              ("s3://{bucket}/data/kafka/payments", "iceberg_catalog.default.payments_raw")]:  # bucket from the environment, joined to the one bucket with that path
        assert e in edges, f"missing {e}: {sorted(x for x in edges if '://' in x[0] + x[1] or 'lake' in x[1])}"
    assert ("s3://{bucket}/data/kafka/orders", "iceberg_catalog.default.payments_raw") not in edges, "each call keeps its own lineage"
    for e in [("raw_src.events", "via_macro"), ("customers", "via_macro"), ("crm_src.segments", "via_macro")]:  # dbt: a macro's argument, a macro's body, a macro's macro
        assert e in edges, f"missing {e}"
    assert not any("never_read" in s for s, _ in edges), "a commented-out ref is no edge"
    for e in [("raw.clicks", "lakehouse.silver.clicks_clean"), ("raw.sessions", "lakehouse.silver.sessions_clean"),  # argparse
              ("raw.a", "stg.a"), ("raw.b", "stg.b"), ("raw.c", "stg.c"), ("raw.d", "stg.d")]:  # per call, per loop iteration
        assert e in edges, f"missing {e}"
    assert not {("raw.clicks", "lakehouse.silver.sessions_clean"), ("raw.a", "stg.b"), ("raw.c", "stg.d")} & edges, "no crossover"
    for e in [("finance.invoices_raw", "finance.invoices"), ("finance.refunds_raw", "finance.refunds"),  # rows of a config file
              ("silver.orders", "gold.orders"), ("stage.users", "pub.users"), ("stage.teams", "pub.teams")]:  # click; another file's calls
        assert e in edges, f"missing {e}"
    assert not {("finance.invoices_raw", "finance.refunds"), ("stage.users", "pub.teams")} & edges, "no crossover"
    assert {("src.app_cart", "mart.app_cart"), ("src.app_wishlist", "mart.app_wishlist")} <= edges, "values carried across two files"
    assert ("src.app_cart", "mart.app_wishlist") not in edges
    with sqlite3.connect(repo / ".data-context/index.db") as c:
        assert {"load_invoices", "load_refunds"} <= {r[0] for r in c.execute("select task from runs where dag='factory'")}
    shared = cli("card", "iceberg_catalog.default.payments_raw")  # the shared script runs in two tasks; this table in one
    assert "runs in (1): s3_to_iceberg.load_payments" in shared and "load_orders" not in shared, shared
    assert "s3://raw-bucket/ref" in names and not any("{" in n.replace("{bucket}", "") or n == "directory" or "lake/data" in n or n == "s3://lake/raw" for n in names), sorted(n for n in names if "://" in n)

    # best practices from the index: each rule fires on its planted case, and stays quiet where the practice is followed
    report = cli("lint")
    datawright.ROOT, datawright.HOME = repo, repo / ".data-context"
    with sqlite3.connect(repo / ".data-context/index.db") as c:
        c.row_factory = sqlite3.Row
        found = "\n".join(f"{at}  {msg}" for _, at, msg in datawright.practices(c))
    assert "models/inc_orders.sql:1  inc_orders: incremental model with no unique_key" in found, found
    assert "sessions_clean: appends in silver.sessions" in found, "a shared script's table names its own task"
    for text in ("rerun duplicates rows:", "practices/sql/events.sql:1  dw.events: appends in load.events (0 2 * * *)",
                 "several writers:", "dw.shared: written on a schedule by 2 DAGs (load, report)",
                 "select * into a table:", "practices/sql/events.sql:1  dw.events: written with SELECT *",
                 "lineage cycle:", "dw.loop_a → dw.loop_b feed each other",
                 "reads before upstream is built:", "ads.daily (report 0 1 * * *) reads dw.events, built later in the day (load 0 2 * * *)",
                 "layer order:", "ods.feedback reads ads.daily, from a later layer",
                 "secret in code:", "practices/conn.py:2  credentials in a connection URL"):
        assert text in report, f"lint is missing {text!r}:\n{report}"
    assert "dw.users: appends" not in report, "a keyed table upserts: rerunning its INSERT is safe"
    plan = cli("backfill", "ods.events", "--from", "2026-11-01", "--to", "2026-11-03")  # upstream first, a wave at a time; runs nothing
    for text in ("backfill plan for ods.events and 3 downstream tables, 2026-11-01 to 2026-11-03",
                 "wave 2 (after the wave before has finished)\n  dw.events  load.events  0 2 * * *\n    airflow tasks clear load -t '^events$' -s 2026-11-01 -e 2026-11-03 -y",
                 "wave 3", "  ads.daily  report.daily", "wave 4", "  ods.feedback  report.feedback",
                 "dw.events appends (practices/sql/events.sql:1): delete 2026-11-01 to 2026-11-03 from it first",
                 "after: datawright check ads.daily dw.events ods.events ods.feedback"):
        assert text in plan, f"backfill plan is missing {text!r}:\n{plan}"
    assert "Sup3rS3cret" not in report and "practices/conn.py:1" not in report and "practices/conn.py:3" not in report, "no values; comments and placeholders skipped"

    advice = cli("advise", "wh.events")  # from how the code filters and joins it, against the layout its DDL declares
    for text in ("now: partition event_id", "filtered on: event_date (2), kind (2)", "joined on: user_id (2)",
                 "(filtered in 2 queries; partitioning on it lets them skip the rest)", "partitioned by event_id, which none of the 4 filtering queries here use"):
        assert text in advice, f"advice is missing {text!r}:\n{advice}"
    advice = cli("advise", "fct_events")  # layout from dbt config: already partitioned and clustered the way it's queried
    assert "now: cluster kind,user_id · partition event_date" in advice and "nothing to change" in advice, advice
    advice = cli("advise", "wh.events")
    assert "grouped by: kind (1)" in advice and "wh.events" in cli("advise").split("details:")[0]

    # concepts: what columns mean, from how the code joins and renames them
    listing = cli("concepts")
    assert "player (proposed): home crm.player_accounts.id" in listing and "called player_id (4)" in listing, listing
    ccard = cli("card", "app.sessions")
    assert "joins (1): crm.player_accounts on playerid = id (1)" in ccard and "playerid → player (proposed) · home crm.player_accounts.id" in ccard, ccard
    assert "unused keys: device_ref: no code here reads or joins it; the code joins this table on playerid" in ccard, ccard
    assert "member_ref → player" in cli("card", "bi.rollup"), "a rename of a joined key joins its concept"
    assert "rollup" not in cli("card", "bi.rollup").split("means")[1].split("n →")[0] and "n → " not in cli("card", "bi.rollup"), "a count is no concept"
    datawright.ROOT, datawright.HOME = repo, repo / ".data-context"
    with sqlite3.connect(repo / ".data-context/index.db") as c:  # drift: one concept, another kind of type and a respelled name
        c.row_factory = sqlite3.Row
        drift = "\n".join(f"{r} | {at} | {msg}" for r, at, msg in datawright.practices(c) if r.startswith("concept"))
    assert "concept type differs | concepts/ddl.sql:3 | app.sessions.playerid is VARCHAR(36), but player is BIGINT in 3 other tables" in drift, drift
    assert "concept name differs | concepts/ddl.sql:3 | app.sessions.playerid holds player, which the team calls player_id" in drift, drift
    assert "uid holds" not in drift and "member_ref holds" not in drift, "other names aren't drift until a person confirms the concept"
    with sqlite3.connect(repo / ".data-context/index.db") as c:  # MyBatis: dynamic SQL rendered, every <choose> branch read
        m = "service/src/main/resources/mapper/PlayerMapper.xml"
        assert {"crm.player_accounts", "fin.wallet", "fin.bets"} <= {r[0] for r in c.execute("select dataset from readers where file=?", (m,))}
        assert c.execute("select 1 from joins where file=? and left_column='player_id' and right_dataset='crm.player_accounts'", (m,)).fetchone()
        assert not c.execute("select 1 from issues where file=?", (m,)).fetchone() and not c.execute("select 1 from datasets where name like '%jinja%'").fetchone()
    vcard = cli("card", "fin.requests", "--budget", "3000")  # value maps from CASE, searched CASE and DECODE; conversions into outputs
    for text in ("status: 1 = pending, 2 = approved, 3 = rejected (3 places in the code)", "type: 1 = ≈ deposit (1 place in the code)",
                 "amount_cents → rep.requests.amount_usd (÷ 100)", "created_ms → rep.requests.created (epoch milliseconds to time)",
                 "created_at → rep.requests.created_local (UTC to Asia/Manila)"):
        assert text in vcard, f"card is missing {text!r}:\n{vcard}"
    assert "amount_usd ← fin.requests.amount_cents ÷ 100" in cli("card", "rep.requests", "--budget", "3000")
    with sqlite3.connect(repo / ".data-context/index.db") as c:
        c.row_factory = sqlite3.Row
        labels = [msg for r, at, msg in datawright.practices(c) if r == "value labels differ"]
    assert labels == ["fin.requests.status = 3 is labelled 'declined' here but 'rejected' in 2 other places: reports will disagree"], labels
    path = cli("join", "fin.wallet", "app.sessions")
    assert "join crm.player_accounts t1 on t1.id = t0.player_id" in path and "join app.sessions t2 on cast(t2.playerid as bigint) = t1.id" in path, path
    cli("concepts", "--write")
    yml = (repo / ".data-context/concepts.yml").read_text()
    assert "- name: player\n  confirmed: false" in yml and "home: crm.player_accounts.id" in yml, yml
    (repo / ".data-context/concepts.yml").write_text(yml.replace("- name: player\n  confirmed: false\n  description: ''",
                                                                  "- name: member\n  confirmed: true\n  description: A registered player"))
    (repo / "concepts/deposits.sql").write_text("insert overwrite table bi.deposits select d.member_id from pay.deposits d join crm.player_accounts p on p.id = d.member_id;\n")
    cli("build")
    ccard = cli("card", "app.sessions")
    assert "playerid → member · home crm.player_accounts.id" in ccard and "(proposed)" not in ccard.split("means")[1].split("\n")[0], ccard
    assert "member_id → member (proposed)" in cli("card", "pay.deposits"), "newly joined to a confirmed concept: proposed for it"
    ecard = cli("card", "fct_events", "--budget", "3000")  # a dbt semantic layer's entities: confirmed concepts from the start
    assert "user_id → account · home customers.id" in ecard, ecard
    cli("export", "datahub")
    gl = (repo / ".data-context/exports/datahub_business_glossary.yml").read_text()
    assert "version: '1'" in gl and "source: DataHub" in gl and "- name: member" in gl and "A registered player Home: crm.player_accounts.id" in gl, gl
    assert "- name: player\n" not in gl, "only confirmed concepts unless --all"
    cli("export", "openmetadata", "--all")
    om = (repo / ".data-context/exports/openmetadata_glossary.csv").read_text().splitlines()
    assert om[0] == "parent,name*,displayName,description*,synonyms,relatedTerms,references,tags", om[0]
    assert any(r.startswith(",member,Member,") and "playerid" in r for r in om), om
    assert "app.sessions,playerid,member" in (repo / ".data-context/exports/openmetadata_column_terms.csv").read_text()
    cli("export", "dbt", "--all")
    sm = (repo / ".data-context/exports/datawright_semantic_models.yml").read_text()
    assert "model: ref('customers')" in sm and "name: account\n    type: primary\n    expr: id" in sm, sm
    cli("concepts", "--write")
    yml = (repo / ".data-context/concepts.yml").read_text()
    assert "description: A registered player" and "  - pay.deposits.member_id" in yml.split("  proposed:")[1].split("- name:")[0], yml

    # BI lineage: a warehouse column reaches the Looker tile that shows it, through the view, a measure and the explore
    imp = cli("impact", "mart.revenue_daily.revenue")
    assert "looker:view.revenue.total_revenue" in imp and "looker:dashboard.sales.revenue_tile" in imp, imp
    imp = cli("impact", "mart.revenue_daily")
    assert all(n in imp for n in ("looker:explore.revenue", "looker:dashboard.sales", "looker:view.top_days")), imp
    assert "order_date" in cli("card", "looker:view.top_days") and "order_week" in cli("card", "looker:view.revenue")
    imp = cli("impact", "mart.revenue_daily.revenue")  # Superset: column -> dataset metric -> chart -> dashboard tile
    assert "superset:chart.revenue_trend.total_revenue" in imp and "superset:dashboard.finance.revenue_trend" in imp, imp
    assert not (repo.parent / "escaped.txt").exists() and not (repo / ".data-context/escaped.txt").exists()
    # Metabase: the query builder (legacy and staged), SQL with optional filters, questions built on a model
    assert all(n in imp for n in ("metabase:question.revenue_by_day.revenue", "metabase:model.revenue_sql.revenue",
                                  "metabase:dashboard.exec_kpis.revenue_by_day", "metabase:dashboard.exec_kpis.weekly_revenue",
                                  "metabase:dashboard.exec_kpis.revenue_table")), imp  # through the model it's built on
    assert "metabase:question.old" not in cli("search", "old") and "metabase:model.revenue_sql" in cli("lineage", "metabase:question.weekly_revenue", "--up", "1", "--down", "0")
    assert "correction [revenue]: Excludes test orders  (CMS-1234)" in card and "grain: one row per order_date" in card, card
    assert "description: daily revenue" in card and "grain: one row per order_date" in (repo / ".data-context/notes/mart.revenue_daily.yml").read_text()

    log = [json.loads(line) for line in (repo / ".data-context/usage.jsonl").read_text().splitlines()]
    cards = [e for e in log if e["cmd"] == "card" and e["target"] == "mart.revenue_daily"]
    first_card, second_card = cards[0], cards[1]
    assert "pipelines/revenue.sql" in first_card["paths"] and first_card["baseline"] > 0, first_card
    assert "pipelines/revenue.sql" not in second_card["paths"], "a file must count once an hour"
    report = cli("savings")
    assert "questions answered: 24" in report and "knowledge captured: 1 notes, 1 datasets described" in report, report
    print(report)
    edges, cols = set(con.execute("select src, dst, via from edges")), set(con.execute("select * from col_edges"))
    for src, dst, via in [("mart.revenue_daily", "bi.revenue_dashboard", "import openlineage"),
                          ("mart.revenue_daily", "reporting.finance_extract", "import datahub"), ("mart.revenue_daily", "ml.revenue_forecast", "import openmetadata")]:
        assert (src, dst, via) in edges, f"missing catalog edge {src} -> {dst}"
    for c in [("mart.revenue_daily.revenue", "bi.revenue_dashboard.revenue"), ("mart.revenue_daily.revenue", "reporting.finance_extract.amount"),
              ("mart.revenue_daily.revenue", "ml.revenue_forecast.forecast")]:
        assert c in cols, f"missing catalog column edge {c}"
    forecast = cli("card", "revenue_forecast")
    assert "description: Next 30 days of revenue" in forecast and "mart.revenue_daily (openmetadata)" in forecast and "forecast DECIMAL" in forecast, forecast
    assert "reporting.finance_extract.amount" in cli("lineage", "mart.revenue_daily.revenue", "--up", "0", "--down", "2")
    uses = set(con.execute("select * from col_uses"))
    assert ("staging.payments", "order_id", "pipelines/revenue.sql", 2) in uses, "a join key is a use even though no column copies it"
    assert ("raw.orders", "*", "dags/load_orders.py", 2) in uses and ("staging.orders", "order_date", "reports/weekly.py", 1) not in uses
    hit = cli("impact", "staging.payments.order_id")
    assert "code that reads it (any clause: select, join, where) (1): pipelines/revenue.sql:2" in hit, hit
    hit = cli("impact", "raw.orders.status")
    assert "reads every column (select *, breaks on drops and reorders) (1): dags/load_orders.py:2" in hit, hit
    hit = cli("impact", "mart.revenue_daily.revenue")
    assert "pipelines/revenue.sql:2" in hit and "reports/weekly.py:1" in hit and "bi.revenue_dashboard.revenue (openlineage)" in hit, hit

    # MCP server: a new pipeline lands after the last build, and the warehouse CLI is a stand-in that prints the SQL it gets
    (repo / "pipelines/top.sql").write_text("insert into mart.top_days select order_date from mart.revenue_daily;\n")
    with sqlite3.connect(repo / ".data-context/index.db") as c:
        c.execute("update meta set v='0' where k='built_at'")
    (repo / ".data-context/config.yml").write_text(f'query_command: {sys.executable} -c "import sys; print(sys.argv[1])" {{sql}}  # echo\n')
    calls = [("card", {"dataset": "top_days", "budget": None}), ("card", {"dataset": "nope"}),
             ("query", {"sql": "select order_date from mart.revenue_daily -- why\norder by 1"}),
             ("query", {"sql": "select 1; delete from mart.revenue_daily"}), ("query", {"sql": "select * into t2 from mart.revenue_daily"}),
             ("code", {"dataset": "revenue_daily"}), ("note", {"dataset": "../../../evil", "text": "x"})]
    reqs = [{"id": 1, "method": "initialize", "params": {"protocolVersion": "2025-06-18", "capabilities": {}, "clientInfo": {"name": "t", "version": "0"}}},
            {"method": "notifications/initialized"}, {"id": 2, "method": "tools/list"}, {"id": 3, "method": "nope"},
            *({"id": 10 + i, "method": "tools/call", "params": {"name": n, "arguments": a}} for i, (n, a) in enumerate(calls))]
    lines = "".join(json.dumps({"jsonrpc": "2.0", **r}) + "\n" for r in reqs) + "not json\n"
    out = subprocess.run([sys.executable, datawright.__file__, "serve"], input=lines, capture_output=True, text=True, cwd=repo, timeout=60).stdout
    replies = {m["id"]: m for m in map(json.loads, out.splitlines())}  # stdout must hold nothing but JSON-RPC
    assert len(replies) == 11 and replies[None]["error"]["code"] == -32700 and replies[3]["error"]["code"] == -32601, out
    assert replies[1]["result"]["protocolVersion"] == "2025-06-18" and replies[1]["result"]["serverInfo"]["name"] == "datawright"
    assert {t["name"] for t in replies[2]["result"]["tools"]} == {"card", "code", "impact", "join", "advise", "backfill", "lineage", "search", "note", "set", "query"}
    text = lambda i: replies[i]["result"]["content"][0]["text"]
    assert not replies[10]["result"]["isError"] and "produced by pipelines/top.sql:1" in text(10), "rebuilds when a commit is newer"
    assert replies[11]["result"]["isError"]
    assert "LIMIT 100" in text(12) and "why" not in text(12), "runs the parsed SELECT with a row cap and no comments"
    assert all(replies[i]["result"]["isError"] and "Rejected" in text(i) for i in (13, 14)), [text(13), text(14)]
    assert text(15).startswith("pipelines/revenue.sql:1-6\n-- daily revenue\ninsert into mart.revenue_daily"), text(15)
    assert not (repo.parent / "evil.yml").exists() and (repo / ".data-context/notes/.._.._.._evil.yml").exists(), "a name is never a path"
    assert any(json.loads(line)["target"] == "mart.top_days" for line in (repo / ".data-context/usage.jsonl").read_text().splitlines())
    # review: a branch that changes the revenue model, as a merge request would
    assert "tests" not in cli("card", "customers").split("missing:")[-1].splitlines()[0], "dbt data_tests count as tests"
    cli("set", "revenue_daily", "tier=0")
    assert "tier 0" in cli("card", "revenue_daily") and "mart.revenue_daily" in cli("lint").split("least ready")[1]
    git("checkout", "-qb", "CMS-1234-revenue-refunds")
    (repo / "pipelines/revenue.sql").write_text((repo / "pipelines/revenue.sql").read_text().replace("sum(p.amount)", "sum(p.amount) - 0"))
    (repo / "pipelines/new_broken.sql").write_text("insert into mart.x select from where;\n")
    (repo / "pipelines/settings.py").write_text('DSN = "mysql://loader:Hunter2pass@wh:3306/ods"\n')
    (repo / "pipelines/visits.sql").write_text("insert overwrite table bi.visits select v.playerid from app.visits v join crm.player_accounts p on p.id = v.playerid;\n")
    (repo / "dags/etl.py").write_text((repo / "dags/etl.py").read_text().replace("hours=6", "hours=4"))
    git("add", "-A")
    git("commit", "-qm", "CMS-1234 net refunds out of revenue")
    cli("build")
    with contextlib.suppress(SystemExit):
        cli("review", "--base", "HEAD~1", "--strict")
    report = cli("review", "--base", "HEAD~1")
    assert "text:" not in report, "parse errors never carry the SQL's text (sqlglot's token reprs quote it)"
    for text in ("## Data impact of `CMS-1234-revenue-refunds`", "`pipelines/revenue.sql` | etl.build_revenue `timedelta(hours=4)` |",
                 "mart.top_days", "bi.revenue_dashboard (openlineage)", "`reports/weekly.py`", "Refunds land three days late",
                 "doesn't parse (1)", "`pipelines/new_broken.sql:1`", "Ticket: CMS-1234", "| `mart.revenue_daily` **tier 0** |", "**Critical tables reached:** `mart.revenue_daily` (tier 0)", "**Best practices (5):**", "bi.deposits.member_id holds member, which the team calls player_id: rename it, or add member_id to its roles in concepts.yml", "- secret in code: `pipelines/settings.py:1`", "- concept name differs: `pipelines/visits.sql:1` bi.visits.playerid holds member, which the team calls player_id", "**Contract broken (1):**", "**Dashboards reached (4):** `looker.revenue_board`, `looker:dashboard.sales`, `metabase:dashboard.exec_kpis`, `superset:dashboard.finance`", "DAGs changed: `adhoc` `manual` (1 tasks), `etl` `timedelta(hours=4)`"):
        assert text in report, f"review is missing {text!r}:\n{report}"
    git("checkout", "-q", "--detach")  # as a merge request pipeline checks out
    os.environ.update(CI_MERGE_REQUEST_SOURCE_BRANCH_NAME="feature/CMS-777-x", CI_MERGE_REQUEST_TITLE="CMS-777 revenue")
    assert "Ticket keys differ:** branch CMS-777, commits CMS-1234, CMS-777" in cli("review", "--base", "HEAD~1")
    del os.environ["CI_MERGE_REQUEST_SOURCE_BRANCH_NAME"], os.environ["CI_MERGE_REQUEST_TITLE"]
    try:
        cli("review", "--base", "HEAD~1", "--strict")
        raise AssertionError("--strict must fail on SQL that doesn't parse")
    except SystemExit as e:
        assert e.code == 1

    # updates: a newer copy of the skill installed in the repo is found, offered after a build, and copied in
    newer = repo / ".claude/skills/data-context/scripts/datawright.py"
    newer.parent.mkdir(parents=True)
    newer.write_text(Path(datawright.__file__).read_text().replace(f'VERSION = "{datawright.VERSION}"', 'VERSION = "99.0.0"'))
    assert "a newer datawright is installed (99.0.0): run datawright update" in cli("build")
    assert "-> 99.0.0" in cli("update") and datawright.version_of((repo / ".data-context/datawright.py").read_text()) == (99, 0, 0)
    assert "is up to date" in cli("update")
    # the engine a repo declares
    hint_repo = Path(tempfile.mkdtemp())
    (hint_repo / "requirements.txt").write_text("dbt-trino==1.8.0\n")
    datawright.ROOT = hint_repo
    assert datawright.dialect_hint(["requirements.txt", "models/a.sql"]) == "trino"
    assert datawright.dialect_hint(["a.hql", "b.hql", "c.sql"]) == "hive" and datawright.dialect_hint(["a.sql"]) is None
    fresh = Path(tempfile.mkdtemp())  # init installs the command, a short `dw` wrapper and the index in one go
    (fresh / "a.sql").write_text("create table m.t as select 1 as x;\n")
    subprocess.run(["git", "init", "-q"], cwd=fresh, check=True)
    init = subprocess.run([sys.executable, str(Path(__file__).parents[1] / "skills/data-context/scripts/datawright.py"), "init"], cwd=fresh, capture_output=True, text=True)
    assert init.returncode == 0 and "installed in .data-context/" in init.stdout, init.stdout + init.stderr
    short = subprocess.run([str(fresh / ".data-context/dw"), "card", "m.t"], cwd=fresh, capture_output=True, text=True)
    assert short.returncode == 0 and "m.t" in short.stdout, short.stdout + short.stderr
    print("datawright self-check passed")


if __name__ == "__main__":
    main()
