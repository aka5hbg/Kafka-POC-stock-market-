import requests
import pandas as pd
import pyarrow.parquet as pq
from datetime import datetime

# Replace 'YOUR_API_KEY' with your actual Alpha Vantage API key
API_KEY = 'xyz'

def fetch_historical_data(symbol, start_date, end_date):
    url = 'https://www.alphavantage.co/query'
    params = {
        'function': 'TIME_SERIES_DAILY',
        'symbol': symbol,
        'apikey': API_KEY,
        'outputsize': 'full',  # 'compact' for last 100 data points, 'full' for complete dataset
        'datatype': 'json',
    }
    
    response = requests.get(url, params=params)
    if response.status_code == 200:
        data = response.json().get('Time Series (Daily)', None)
        if data:
            df = pd.DataFrame(data).transpose()
            df.index = pd.to_datetime(df.index)
            df.sort_index(inplace=True)
            df['symbol'] = symbol
            return df
        else:
            print(f"No data found for {symbol}.")
            return None
    else:
        print(f"Failed to fetch data for {symbol}. Status code: {response.status_code}")
        return None

if __name__ == "__main__":
    symbols = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'FB']  # Example symbols for Apple, Microsoft, Google, Amazon, Facebook
    start_date = '2023-01-01'
    end_date = '2023-06-30'
    
    all_data = []
    
    for symbol in symbols:
        historical_data = fetch_historical_data(symbol, start_date, end_date)
        
        if historical_data is not None:
            all_data.append(historical_data)
            print(f"Successfully fetched data for {symbol}.")
        else:
            print(f"Failed to fetch data for {symbol}.")
    
    if all_data:
        combined_df = pd.concat(all_data)
        combined_df.reset_index(inplace=True)
        
        # Write to Parquet file
        file_name = 'tech_company_stock_data.parquet'
        pq.write_table(pa.Table.from_pandas(combined_df), file_name)
        print(f"Data saved to {file_name} successfully.")
