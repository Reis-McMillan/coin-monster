import json
import asyncio
import pathlib

from config import config
from utils.make_fixtures import FixtureWebsocket

# list of expected .json fixtures
# developer should set this themselves
_CHANNELS_MAP = {
    "candles.json": "candles",
    "heartbeats.json": "heartbeats",
    "l2_data.json": "level2",
    "market_trades.json": "market_trades",
    "subscriptions.json": "subscriptions",
    "ticker.json": "ticker",
    "websocket.json": "websocket_all"
}
_FIXTURES_DIR = pathlib.Path('tests/fixtures')

_fixtures_json = {p.name for p in _FIXTURES_DIR.glob('*.json')}
_missing_json = set(_CHANNELS_MAP.keys()) - _fixtures_json

if len(_missing_json) > 0:
    print("Missing fixtures... making fixtures. Wait one minute.")
    fixture_ws = FixtureWebsocket(
        coins=config.COINS,
        channels=['heartbeats', 'candles', 'level2', 'market_trades', 'ticker']
    )
    asyncio.run(fixture_ws.make_fixtures())

for k, v in _CHANNELS_MAP.items():
    with open(f"{str(_FIXTURES_DIR)}/{k}") as f:
        globals()[v] = json.load(f)
