# /// script
# requires-python = ">=3.10"
# dependencies = ["sqlglot>=30", "pyyaml>=6"]
# ///
"""Self-check for streaming lineage across repos: Kafka topics linked through Flink SQL, Java and Scala jobs, StarRocks Routine Load,
Spark Structured Streaming and the Airflow DAG that submits the Flink job. Run: uv run tests/test_streaming.py"""
import shutil, subprocess, sys, tempfile
from pathlib import Path

DCTX = Path(__file__).parents[1] / "skills/data-context/scripts/dctx.py"
PLATFORM = {  # the Airflow repo, shaped like a real one: DAGs submit Flink jars by class name
    "dags/kafka_to_starrocks_orders.py": '''from airflow import DAG
from airflow.operators.python import PythonOperator

flink_job_class = "com.acme.jobs.OrdersToStarrocks"


def submit():
    requests.post(f"{FLINK_URL}/jars/{jar_id}/run", json={"entryClass": flink_job_class})


with DAG(dag_id="kafka_to_starrocks_orders", schedule="*/10 * * * *") as dag:
    PythonOperator(task_id="submit_flink_job", python_callable=submit)
''',
    "flink/enrich_orders.sql": """CREATE TABLE orders_src (order_id BIGINT, amount DECIMAL(12, 2)) WITH (
  'connector' = 'kafka', 'topic' = 'shop.orders', 'properties.bootstrap.servers' = 'broker:9092',
  'properties.sasl.jaas.config' = 'username="svc" password="kafka-secret-pw"'
);
CREATE TABLE orders_enriched_sink (order_id BIGINT, amount DECIMAL(12, 2)) WITH ('connector' = 'upsert-kafka', 'topic' = 'shop.orders.enriched');
INSERT INTO orders_enriched_sink SELECT order_id, amount * 1.1 AS amount FROM orders_src;
""",
    "starrocks/routine_load.sql": """CREATE ROUTINE LOAD dw.load_orders_enriched ON ods.orders_enriched
COLUMNS TERMINATED BY ',' PROPERTIES ("desired_concurrent_number" = "1")
FROM KAFKA ("kafka_broker_list" = "broker:9092", "kafka_topic" = "shop.orders.enriched");
""",
    "spark/payments_stream.py": 'spark.readStream.format("kafka").option("subscribe", "shop.payments").load()'
                                '.writeStream.toTable("ods.payments_stream")\n',
}
JOBS = {  # the jar repo
    "src/main/java/com/acme/jobs/OrdersToStarrocks.java": '''package com.acme.jobs;

public class OrdersToStarrocks {
    static final String TOPIC = "shop.orders";

    public static void main(String[] args) throws Exception {
        KafkaSource<String> source = KafkaSource.<String>builder().setTopics(TOPIC).build();
        SinkFunction<String> sink = StarRocksSink.sink(StarRocksSinkOptions.builder()
            .withProperty("database-name", "dw")
            .withProperty("table-name", "fact_orders").build());
    }
}
''',
    "src/main/scala/com/acme/jobs/DailyOrders.scala": '''object DailyOrders {
  val Target = "dw.daily_orders"
  def main(args: Array[String]): Unit = {
    tEnv.executeSql("INSERT INTO " + Target + """
      SELECT order_date, count(*) AS orders FROM dw.fact_orders GROUP BY order_date""")
  }
}
''',
    "src/main/java/com/acme/jobs/Config.java": 'public class Config {\n    public static final String PAYMENTS_TOPIC = "shop.payments.raw";\n'
                                               '    public static final String SR_DB = "ods";\n'
                                               '    public static final String ORDER_EVENTS_TOPIC = System.getenv("KAFKA_TOPIC_ORDER_EVENTS");\n'
                                               '    public static final String VIP_TOPIC = System.getenv("KAFKA_TOPIC_VIP");\n}\n',
    "deploy/flink-env.yaml": "env:\n  - name: KAFKA_TOPIC_ORDER_EVENTS\n    value: shop.order.events\n",
    "src/main/java/com/acme/jobs/EventsToStarrocks.java": '''public class EventsToStarrocks {
    static final String SQL = "CREATE TABLE src (id BIGINT) WITH ('connector' = 'kafka', 'topic' = '%s')";
    public static void main(String[] args) {
        tEnv.executeSql(String.format(SQL, requireEnv("EVENTS_TOPIC", Config.ORDER_EVENTS_TOPIC)));
        tEnv.executeSql("CREATE TABLE sink (id BIGINT) WITH ('connector' = 'starrocks', 'database-name' = 'ods', 'table-name' = 'order_events')");
        tEnv.executeSql("INSERT INTO sink SELECT id FROM src");
        tEnv.executeSql(String.format("CREATE TABLE vip (id BIGINT) WITH ('connector' = 'kafka', 'topic' = '%s')", Config.VIP_TOPIC));
        tEnv.executeSql("CREATE TABLE vip_sink (id BIGINT) WITH ('connector' = 'starrocks', 'database-name' = 'ods', 'table-name' = 'vip')");
        tEnv.executeSql("INSERT INTO vip_sink SELECT id FROM vip");
    }
}
''',
    "src/main/java/com/acme/jobs/PaymentsToStarrocks.java": '''public class PaymentsToStarrocks {
    static final String SOURCE = """
        CREATE TABLE src (id BIGINT) WITH ('connector' = 'kafka', 'topic' = '%s',
          'properties.sasl.jaas.config' = 'PlainLoginModule required password="%s";')""";
    static final String SINK = """
        CREATE TABLE sink (id BIGINT) WITH ('connector' = 'starrocks', 'database-name' = '%s', 'table-name' = 'payments')""";
    public static void main(String[] args) {
        String topic = requireEnv("PAYMENTS_TOPIC", Config.PAYMENTS_TOPIC);
        tEnv.executeSql(String.format(SOURCE, topic, args[0]));
        tEnv.executeSql(String.format(SINK, Config.SR_DB));
        tEnv.executeSql("INSERT INTO sink SELECT id FROM src");
    }
}
''',
    "src/main/java/com/acme/jobs/RefundsToStarrocks.java": '''public class RefundsToStarrocks {
    public static void main(String[] args) {
        tEnv.executeSql(String.format("CREATE TABLE src (id BIGINT) WITH ('connector' = 'kafka', 'topic' = '%s')", params.get("topic")));
        tEnv.executeSql("CREATE TABLE sink (id BIGINT) WITH ('connector' = 'starrocks', 'database-name' = 'ods', 'table-name' = 'refunds')");
        tEnv.executeSql("INSERT INTO sink SELECT id FROM src");
    }
}
''',
}


def repo(base, name, files):
    root = base / name
    for path, text in files.items():
        (root / path).parent.mkdir(parents=True, exist_ok=True)
        (root / path).write_text(text)
    for args in (["init", "-q"], ["add", "-A"], ["-c", "user.email=t@t", "-c", "user.name=t", "commit", "-qm", "init"]):
        subprocess.run(["git", *args], cwd=root, check=True, capture_output=True)
    return root


def main():
    base = Path(tempfile.mkdtemp())
    platform, _ = repo(base, "platform", PLATFORM), repo(base, "jobs", JOBS)
    (platform / ".data-context").mkdir()
    shutil.copy(DCTX, platform / ".data-context")
    (platform / ".data-context/config.yml").write_text("repos: [../jobs]\n")
    dctx = lambda *a: subprocess.run([sys.executable, ".data-context/dctx.py", *a], cwd=platform, capture_output=True, text=True)

    built = dctx("build")
    print(built.stdout)
    assert built.returncode == 0 and "kafka-secret-pw" not in built.stdout + built.stderr, built.stderr
    import sqlite3
    con = sqlite3.connect(platform / ".data-context/index.db")
    edges = {(s, d) for s, d in con.execute("select src, dst from edges")}
    for e in [("kafka:shop.orders", "dw.fact_orders"),                  # Java Flink job: Kafka source -> StarRocks sink
              ("kafka:shop.orders", "kafka:shop.orders.enriched"),      # Flink SQL connector tables stand for their topics
              ("kafka:shop.orders.enriched", "ods.orders_enriched"),    # StarRocks Routine Load
              ("kafka:shop.payments", "ods.payments_stream"),           # Spark Structured Streaming
              ("dw.fact_orders", "dw.daily_orders"),                    # Scala job, SQL split across literals and a constant
              ("kafka:shop.payments.raw", "ods.payments"),              # String.format template filled from another class
              ("kafka:shop.order.events", "ods.order_events"),          # Config.X = System.getenv(...), set in a deploy file
              ("kafka:env:kafka_topic_vip", "ods.vip")]:                # ... and set nowhere in the repos: named after the variable
        assert e in edges, f"missing {e}: {sorted(edges)}"
    assert "kafka-secret-pw" not in str(con.execute("select * from issues").fetchall())
    names = [n for (n,) in con.execute("select name from datasets")]
    assert not any("jinja" in n or n in ("src", "sink") for n in names), f"placeholders and job-local table names are not datasets: {names}"
    assert ("kafka:?refundstostarrocks", "ods.refunds") in edges, "a topic set at run time is that job's own placeholder"

    card = dctx("card", "fact_orders").stdout
    for text in ("produced by ../jobs/src/main/java/com/acme/jobs/OrdersToStarrocks.java:",
                 "runs in (1): kafka_to_starrocks_orders.submit_flink_job */10 * * * *", "upstream (1): kafka:shop.orders"):
        assert text in card, f"card is missing {text!r}:\n{card}"
    down = dctx("lineage", "shop.orders", "--up", "0", "--down", "9").stdout
    assert all(t in down for t in ("dw.fact_orders", "dw.daily_orders", "ods.orders_enriched")), down
    assert "setTopics(TOPIC)" in dctx("code", "fact_orders").stdout, "code reads files in the extra repo"
    plan = dctx("backfill", "dw.fact_orders", "--from", "2026-11-01", "--to", "2026-11-02").stdout
    assert "airflow tasks clear kafka_to_starrocks_orders -t '^submit_flink_job$'" in plan and "dw.fact_orders is fed from Kafka" in plan, plan
    print(card)

    # a topic variable set outside the repos (a Helm chart elsewhere): coverage says so, and its value goes in config.yml
    assert "names set by environment variables at deploy time: 1, e.g. kafka:env:kafka_topic_vip" in dctx("coverage").stdout
    cfg = platform / ".data-context/config.yml"
    cfg.write_text(cfg.read_text() + "env:\n  KAFKA_TOPIC_VIP: shop.vip\n  KAFKA_TOPIC_VIPS: shop.vips\n")
    dctx("build")
    with sqlite3.connect(platform / ".data-context/index.db") as con:
        edges = {(s, d) for s, d in con.execute("select src, dst from edges")}
    assert ("kafka:shop.vip", "ods.vip") in edges and ("kafka:env:kafka_topic_vip", "ods.vip") not in edges, sorted(edges)
    cov = dctx("coverage").stdout
    assert "deploy time" not in cov and "values given for variables no code reads: 1, e.g. KAFKA_TOPIC_VIPS" in cov, cov
    (platform / ".data-context/connectors").mkdir(exist_ok=True)  # or a connector reads them from the team's own docs
    (platform / ".data-context/connectors/topics.py").write_text("def match(path):\n    return path.endswith('deploy/flink-env.yaml')\n\n"
                                                                  "def extract(path, text):\n    yield {'env': ['KAFKA_TOPIC_VIP', 'shop.vip.v2']}\n")
    cfg.write_text("repos: [../jobs]\n")
    dctx("build")
    with sqlite3.connect(platform / ".data-context/index.db") as con:
        assert con.execute("select 1 from edges where src='kafka:shop.vip.v2' and dst='ods.vip'").fetchone()
    print("streaming self-check passed")


if __name__ == "__main__":
    main()
