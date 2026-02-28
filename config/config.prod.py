import base64
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
KEY_NAME , KEY_SECRET = _get_key()
WS_API_URL = "wss://advanced-trade-ws.coinbase.com"
LOGGING_ENABLED = True
OPENOBSERVE_ENDPOINT = os.environ.get('OPENOBSERVE_ENDPOINT')
_oo_user = os.environ.get('OPENOBSERVE_USER')
_oo_token = os.environ.get('OPENOBSERVE_TOKEN')
OPENOBSERVE_TOKEN = base64.b64encode(f"{_oo_user}:{_oo_token}".encode()).decode() if _oo_user and _oo_token else None