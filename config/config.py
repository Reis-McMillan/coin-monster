import json
import os

def _get_key():
    key_path = os.environ.get('CB_API_KEY_PATH')
    if not key_path:
        raise ValueError("CB_API_KEY_PATH environment variable is not set.")
    
    with open(key_path, 'r') as f:
        key_data = json.load(f)
    
    return key_data['name'], key_data['privateKey']

COINS = ['BTC-USD']
DB = 'qdb'
DB_HOST = os.environ.get('DB_HOST')
DB_PORT = os.environ.get('DB_PORT')
DB_USER = os.environ.get('DB_USER')
DB_PASS = os.environ.get('DB_PASS')
LOOKBACK_WINDOW = 100
FORECAST_NUM = 5
GRANULARITY = 300
KEY_NAME , KEY_SECRET = _get_key()
WS_API_URL = "wss://advanced-trade-ws.coinbase.com"