import sys, boto3, json
from datetime import datetime, timezone
from awsglue.utils import getResolvedOptions
import pyodbc
import pandas as pd

args = getResolvedOptions(sys.argv, ["JOB_NAME", "RAW_BUCKET"])
RAW_BUCKET = args["RAW_BUCKET"]

secrets = boto3.client("secretsmanager")
s3 = boto3.client("s3")

def get_sql_server_credentials():
    secret = secrets.get_secret_value(SecretId="globalpartners/sqlserver-creds")
    return json.loads(secret["SecretString"])

def get_connection():
    creds = get_sql_server_credentials()
    conn_str = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={creds['host']},{creds['port']};"
        f"DATABASE={creds['database']};"
        f"UID={creds['username']};"
        f"PWD={creds['password']};"
        f"Encrypt=yes;TrustServerCertificate=no;"
        )
    return pyodbc.connect(conn_str)

# Tables to extract, with an incremental watermark column where available
TABLES = {
    "order_items": {"watermark_col": "creation_time_utc"},
    "order_item_options": {"watermark_col": None},
    downstream
        "date_dim": {"watermark_col": None}, 
        }
def get_last_watermark(table_name):
    try:
        obj = s3.get_object(Bucket=RAW_BUCKET, Key=f"manifest/{table_name}_watermark.json")
        return json.loads(obj["Body"].read())["last_watermark"]
    except s3.exceptions.NoSuchKey:
        return "2020-01-01T00:00:00" 
    
def extract_table(conn, table_name, config):
    watermark_col = config["watermark_col"]

    if watermark_col:
        last_watermark = get_last_watermark(table_name)
        query = f"""
            SELECT * FROM {table_name}
            WHERE {watermark_col} > '{last_watermark}'
        """
    else:
        query = f"SELECT * FROM {table_name}"

df = pd.read_sql(query, conn)

if df.empty:
    print(f"{table_name}: no new rows since last run")
    return

pulled_at = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
local_path = f"/tmp/{table_name}_{pulled_at}.parquet"
df.to_parquet(local_path, index=False)

s3_key = f"bronze/{table_name}/pulled_at={pulled_at}/{table_name}.parquet"
s3.upload_file(local_path, RAW_BUCKET, s3_key)
print(f"Extracted {len(df):,} rows from {table_name} -> s3://{RAW_BUCKET}/{s3_key}")

# Advance the watermark only after a confirmed successful upload
if watermark_col:
    new_watermark = df[watermark_col].max()
    s3.put_object(
        Bucket=RAW_BUCKET,
        Key=f"manifest/{table_name}_watermark.json",
        Body=json.dumps({"last_watermark": str(new_watermark)})
    )

def main():
    conn = get_connection()
    try:
        for table_name, config in TABLES.items():
            extract_table(conn, table_name, config)
    finally:
        conn.close()

if __name__ == "__main__":
    main()