import time
import logging
import os
import sys
import snowflake.connector
from prometheus_client import start_http_server, Gauge

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("snowflake_exporter")

# Prometheus Metrics
warehouse_credits = Gauge('snowflake_warehouse_credits_used', 'Credits used per warehouse', ['warehouse'])
warehouse_load = Gauge('snowflake_warehouse_load_avg', 'Average warehouse load', ['warehouse'])
query_duration = Gauge('snowflake_query_duration_seconds_avg', 'Average query duration in seconds', ['warehouse'])
table_storage = Gauge('snowflake_table_storage_bytes_used', 'Table storage size in bytes', ['database', 'schema', 'table'])
login_success = Gauge('snowflake_login_success_count', 'Number of successful logins', ['user'])
login_failure = Gauge('snowflake_login_failure_count', 'Number of failed logins', ['user'])
access_events = Gauge('snowflake_access_events_count', 'Table access events count', ['user', 'table'])
session_count = Gauge('snowflake_session_count', 'Active sessions count', ['user'])
failed_queries = Gauge('snowflake_failed_queries_count', 'Number of failed queries', ['warehouse'])

# Config from environment
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USERNAME")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")
EXPORTER_PORT = int(os.getenv("EXPORTER_PORT", "8000"))

# Validate required environment variables
required_env = {
    "SNOWFLAKE_USERNAME": SNOWFLAKE_USER,
    "SNOWFLAKE_PASSWORD": SNOWFLAKE_PASSWORD,
    "SNOWFLAKE_ACCOUNT": SNOWFLAKE_ACCOUNT,
    "SNOWFLAKE_WAREHOUSE": SNOWFLAKE_WAREHOUSE,
    "SNOWFLAKE_DATABASE": SNOWFLAKE_DATABASE,
    "SNOWFLAKE_SCHEMA": SNOWFLAKE_SCHEMA,
}

missing = [k for k, v in required_env.items() if not v]
if missing:
    logger.error(f"❌ Missing required environment variables: {', '.join(missing)}")
    sys.exit(1)

def connect_to_snowflake():
    try:
        conn = snowflake.connector.connect(
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            account=SNOWFLAKE_ACCOUNT,
            warehouse=SNOWFLAKE_WAREHOUSE,
            database=SNOWFLAKE_DATABASE,
            schema=SNOWFLAKE_SCHEMA,
        )
        return conn
    except Exception as e:
        logger.error(f"❌ Snowflake connection failed: {e}")
        return None

def collect_metrics():
    conn = connect_to_snowflake()
    if not conn:
        return

    cs = conn.cursor()
    try:
        # Clear previous values
        warehouse_credits.clear()
        warehouse_load.clear()
        query_duration.clear()
        table_storage.clear()
        login_success.clear()
        login_failure.clear()
        access_events.clear()
        session_count.clear()
        failed_queries.clear()

        # Set context
        cs.execute(f"USE DATABASE {SNOWFLAKE_DATABASE}")
        cs.execute(f"USE SCHEMA {SNOWFLAKE_SCHEMA}")

        # Credits used
        cs.execute("""
            SELECT warehouse_name, SUM(credits_used)
            FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
            WHERE start_time > DATEADD(hour, -1, CURRENT_TIMESTAMP())
            GROUP BY warehouse_name
        """)
        for row in cs.fetchall():
            warehouse_credits.labels(warehouse=row[0]).set(row[1])

        # Warehouse load
        cs.execute("""
            SELECT warehouse_name, AVG(average_running)
            FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_LOAD_HISTORY
            WHERE start_time > DATEADD(hour, -1, CURRENT_TIMESTAMP())
            GROUP BY warehouse_name
        """)
        for row in cs.fetchall():
            warehouse_load.labels(warehouse=row[0]).set(row[1])

        # Query durations
        cs.execute("""
            SELECT warehouse_name, AVG(total_elapsed_time)/1000
            FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
            WHERE start_time > DATEADD(hour, -1, CURRENT_TIMESTAMP())
              AND execution_status = 'SUCCESS'
              AND warehouse_name IS NOT NULL
            GROUP BY warehouse_name
        """)
        for row in cs.fetchall():
            query_duration.labels(warehouse=row[0]).set(row[1])

        # Table storage
        cs.execute("""
            SELECT table_catalog, table_schema, table_name, bytes
            FROM SNOWFLAKE.ACCOUNT_USAGE.TABLE_STORAGE_METRICS
        """)
        for row in cs.fetchall():
            table_storage.labels(database=row[0], schema=row[1], table=row[2]).set(row[3])

        # Login successes
        cs.execute("""
            SELECT user_name, COUNT(*)
            FROM SNOWFLAKE.ACCOUNT_USAGE.LOGIN_HISTORY
            WHERE event_timestamp > DATEADD(hour, -1, CURRENT_TIMESTAMP())
              AND is_success = 'TRUE'
            GROUP BY user_name
        """)
        for row in cs.fetchall():
            login_success.labels(user=row[0]).set(row[1])

        # Login failures
        cs.execute("""
            SELECT user_name, COUNT(*)
            FROM SNOWFLAKE.ACCOUNT_USAGE.LOGIN_HISTORY
            WHERE event_timestamp > DATEADD(hour, -1, CURRENT_TIMESTAMP())
              AND is_success = 'FALSE'
            GROUP BY user_name
        """)
        for row in cs.fetchall():
            login_failure.labels(user=row[0]).set(row[1])

        # Table access events
        cs.execute("""
            SELECT user_name, object_name, COUNT(*)
            FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY
            WHERE event_timestamp > DATEADD(hour, -1, CURRENT_TIMESTAMP())
            GROUP BY user_name, object_name
        """)
        for row in cs.fetchall():
            access_events.labels(user=row[0], table=row[1]).set(row[2])

        # Active sessions
        cs.execute("""
            SELECT user_name, COUNT(*)
            FROM SNOWFLAKE.ACCOUNT_USAGE.SESSIONS
            WHERE logout_time IS NULL
              AND login_time > DATEADD(hour, -1, CURRENT_TIMESTAMP())
            GROUP BY user_name
        """)
        for row in cs.fetchall():
            session_count.labels(user=row[0]).set(row[1])

        # Failed queries
        cs.execute("""
            SELECT warehouse_name, COUNT(*)
            FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
            WHERE start_time > DATEADD(hour, -1, CURRENT_TIMESTAMP())
              AND execution_status = 'FAILED'
              AND warehouse_name IS NOT NULL
            GROUP BY warehouse_name
        """)
        for row in cs.fetchall():
            failed_queries.labels(warehouse=row[0]).set(row[1])

        logger.info("✅ Metrics collected successfully.")
    except Exception as e:
        logger.error(f"❌ Failed to collect metrics: {e}")
    finally:
        cs.close()
        conn.close()

def serve():
    logger.info(f"🚀 Exporter running on port {EXPORTER_PORT}")
    start_http_server(EXPORTER_PORT)
    while True:
        collect_metrics()
        time.sleep(60)

if __name__ == "__main__":
    serve()