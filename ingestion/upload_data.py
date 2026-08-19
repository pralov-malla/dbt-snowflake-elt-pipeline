import os
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from dotenv import load_dotenv

load_dotenv()

ACCOUNT = os.environ.get('SNOWFLAKE_ACCOUNT', '')
USER = os.environ.get('SNOWFLAKE_USER', '')
PASSWORD = os.environ.get('SNOWFLAKE_PASSWORD', '')
ROLE = os.environ.get('SNOWFLAKE_ROLE', 'DBT_ROLE')
WAREHOUSE = os.environ.get('SNOWFLAKE_WAREHOUSE', 'DBT_WH')
DATABASE = os.environ.get('SNOWFLAKE_DATABASE', 'DBT_DB')
SCHEMA = os.environ.get('SNOWFLAKE_SCHEMA', 'RAW')

DATA_DIR = os.path.join(os.path.dirname(__file__), 'data')

CSV_TO_TABLE = {
    'olist_orders_dataset.csv': 'ORDERS',
    'olist_order_items_dataset.csv': 'ORDER_ITEMS',
    'olist_order_payments_dataset.csv': 'ORDER_PAYMENTS',
    'olist_products_dataset.csv': 'PRODUCTS',
}


def main():
    if not PASSWORD:
        print("ERROR: Set your Snowflake password first:")
        print("  export SNOWFLAKE_PASSWORD='your_password'")
        return

    print("Connecting to Snowflake...")
    conn = snowflake.connector.connect(
        user=USER,
        password=PASSWORD,
        account=ACCOUNT,
        role=ROLE,
        warehouse=WAREHOUSE,
        database=DATABASE,
        schema=SCHEMA,
    )
    print("Connected!\n")

    for csv_file, table_name in CSV_TO_TABLE.items():
        file_path = os.path.join(DATA_DIR, csv_file)

        if not os.path.exists(file_path):
            print(f"[SKIP] File not found: {file_path}")
            print(f"   Skipping {table_name}.\n")
            continue

        print(f"Reading {csv_file}...")
        df = pd.read_csv(file_path)
        df.columns = [col.upper() for col in df.columns]

        print(f"Uploading {len(df):,} rows to {DATABASE}.{SCHEMA}.{table_name}...")
        success, nchunks, nrows, _ = write_pandas(
            conn=conn,
            df=df,
            table_name=table_name,
            auto_create_table=True,
            overwrite=True,
        )

        if success:
            print(f"[OK] {table_name}: {nrows:,} rows uploaded.\n")
        else:
            print(f"[FAIL] {table_name}: upload failed.\n")

    # Quick validation
    print("--- Validation ---")
    cur = conn.cursor()
    for table_name in CSV_TO_TABLE.values():
        try:
            cur.execute(f"SELECT COUNT(*) FROM {DATABASE}.{SCHEMA}.{table_name}")
            count = cur.fetchone()[0]
            print(f"  {table_name}: {count:,} rows")
        except Exception as e:
            print(f"  {table_name}: error - {e}")
    cur.close()

    conn.close()
    print("\nDone!")


if __name__ == '__main__':
    main()
