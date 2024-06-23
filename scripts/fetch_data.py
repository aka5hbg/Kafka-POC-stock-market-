import requests
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os
import configparser

# Function to fetch historical data
def fetch_historical_data(symbol, start_date, end_date, api_key):
    url = 'https://www.alphavantage.co/query'
    params = {
        'function': 'TIME_SERIES_DAILY',
        'symbol': symbol,
        'apikey': api_key,
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
    # Read configuration from config.ini
    config = configparser.ConfigParser()
    config.read('config.ini')
    
    api_key = config['AlphaVantage']['api_key']
    symbols = [symbol.strip() for symbol in config['DataParameters']['symbols'].split(',')]
    start_date = config['DataParameters']['start_date']
    end_date = config['DataParameters']['end_date']
    
    all_data = []
    
    for symbol in symbols:
        historical_data = fetch_historical_data(symbol, start_date, end_date, api_key)
        
        if historical_data is not None:
            all_data.append(historical_data)
            print(f"Successfully fetched data for {symbol}.")
        else:
            print(f"Failed to fetch data for {symbol}.")
    
    if all_data:
        combined_df = pd.concat(all_data)
        combined_df.reset_index(inplace=True)
        
        # Specify the directory path (use raw string literal or escape backslashes)
        directory = r'C:\Users\akash\Desktop\historical_stock_data_project\data\raw_data'
        
        # Create the directory if it does not exist
        os.makedirs(directory, exist_ok=True)
        
        # Write to Parquet file
        file_name = 'tech_company_stock_data.parquet'
        file_path = os.path.join(directory, file_name)
        table = pa.Table.from_pandas(combined_df)
        pq.write_table(table, file_path)
        
        print(f"Data saved to {file_path} successfully.")

