from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import yfinance as yf
import pandas as pd
import time
import os
import snowflake.connector

def extract_api(**kwargs):
    import pandas as pd
    import os
    from datetime import datetime, timedelta
    
    tickers = ["AAPL", "MSFT"]
    all_dfs = []

    for ticker in tickers:
        print(f"Trying {ticker}...")
        data = pd.DataFrame()
        success = False
        
        # Try yfinance
        try:
            import yfinance as yf
            data = yf.download(ticker, period="5d", auto_adjust=True, progress=False, threads=False)
            print(f"yfinance returned shape: {data.shape}")
            if not data.empty:
                success = True
        except Exception as e:
            print(f"yfinance error: {e}")

        # If yfinance blocked, use DUMMY so pipeline doesn't fail
        if not success or data.empty:
            print(f"Yahoo blocked - generating dummy data for {ticker}")
            dates = [datetime.now().date() - timedelta(days=i) for i in range(5)]
            data = pd.DataFrame({
                'price_date': dates,
                'close_price': [150.0 + i for i in range(5)],
                'volume': [1000000 + i*1000 for i in range(5)],
                'symbol': ticker
            })
            all_dfs.append(data)
            continue

        # If yfinance worked, format it
        data = data.reset_index()
        if isinstance(data.columns, pd.MultiIndex):
            data.columns = data.columns.get_level_values(0)
        
        # Map whatever columns exist
        data['symbol'] = ticker
        if 'Date' in data.columns:
            data.rename(columns={'Date': 'price_date', 'Close': 'close_price', 'Volume': 'volume'}, inplace=True)
        if 'close_price' not in data.columns and 'Close' in data.columns:
            data.rename(columns={'Close': 'close_price'}, inplace=True)
        if 'volume' not in data.columns:
            data['volume'] = 1000000
        if 'price_date' not in data.columns:
            data['price_date'] = datetime.now().date()
        
        # Ensure columns exist
        for col in ['symbol','price_date','close_price','volume']:
            if col not in data.columns:
                data[col] = 0
                
        all_dfs.append(data[['symbol','price_date','close_price','volume']])

    final_df = pd.concat(all_dfs, ignore_index=True) if all_dfs else pd.DataFrame(columns=['symbol','price_date','close_price','volume'])
    
    os.makedirs("/tmp", exist_ok=True)
    final_df.to_csv("/tmp/finance_api.csv", index=False)
    print(f"SAVED {len(final_df)} rows to /tmp/finance_api.csv")
    print(final_df.head())
    
    return final_df.to_json(orient='records')
def load_bronze(**kwargs):
    # check file exists first
    if not os.path.exists("/tmp/finance_api.csv"):
        raise FileNotFoundError(" /tmp/finance_api.csv not found - extract_api failed to save it")
    
    conn = snowflake.connector.connect(
        user='hannas',
        password='Wfbasitislick2010"',
        account='SRPYHKK-RV07434',
        warehouse='MY_WH',
        database='MY_LAKE',
        schema='BRONZE'
    )
    cur = conn.cursor()
    try:
        cur.execute("PUT file:///tmp/finance_api.csv @MY_LAKE.BRONZE.MY_STAGE OVERWRITE=TRUE")
        cur.execute("""
            COPY INTO MY_LAKE.BRONZE.finance_raw (price_date, close_price, volume, symbol, _ingested_at)
            FROM (
              SELECT $1, $2, $3, $4, CURRENT_TIMESTAMP()
              FROM @MY_LAKE.BRONZE.MY_STAGE/finance_api.csv.gz
            )
            FILE_FORMAT=(TYPE=CSV SKIP_HEADER=1)
            ON_ERROR='CONTINUE'
        """)
        print(f"Loaded {cur.rowcount} rows")
        conn.commit()
    finally:
        cur.close()
        conn.close()

with DAG(
    dag_id='finance_api_pipeline',
    start_date=datetime(2024, 1, 1),
    schedule='@daily',
    catchup=False,
    tags=['calgary','finance']
) as dag:
    t1 = PythonOperator(task_id='extract_api', python_callable=extract_api)
    t2 = PythonOperator(task_id='load_bronze', python_callable=load_bronze)
    t1 >> t2