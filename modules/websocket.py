import asyncio
from collections import defaultdict
from cryptography.hazmat.primitives import serialization
import json
import jwt
from pandera.errors import SchemaError
from questdb.ingress import IngressError
import secrets
import time
from typing import Optional
from voluptuous.error import Invalid
import websockets

from config import config
from db import Base
from modules.order_book import OrderBook

def no_op(*args, **kwargs):
    pass

class Websocket:
    PRIVATE_KEY = serialization.load_pem_private_key(
        config.KEY_SECRET.encode('utf-8'), 
        password=None
    )
    db = None

    @classmethod
    def set_db(cls, db):
        cls.db = db

    def __init__(self, coin: str, channels: list[str]):
        self.coin = coin
        self.channels = channels
        self.order_book = None
        if 'l2_data' in channels:
            self.order_book = OrderBook(coin=self.coin, max_levels=2500)

    def _build_jwt(self):
        jwt_payload = {
            'sub': config.KEY_NAME,
            'iss': "cdp",
            'nbf': int(time.time()),
            'exp': int(time.time()) + 120,
        }
        
        jwt_token = jwt.encode(
            jwt_payload,
            self.PRIVATE_KEY,
            algorithm='ES256',
            headers={'kid': config.KEY_NAME, 'nonce': secrets.token_hex()},
        )
        return jwt_token
    
    def _sign_with_jwt(self, message):
        token = self._build_jwt()
        message['jwt'] = token
        return message

    # --- WebSocket Coroutines ---
    async def _subscribe(self, websocket, channel):
        message = {
            "type": "subscribe",
            "channel": channel,
            "product_ids": [self.coin]
        }
        if channel == 'heartbeats':
            message.pop('product_ids')
        signed_message = self._sign_with_jwt(message)
        await websocket.send(json.dumps(signed_message))

    async def _unsubscribe(self, websocket, channel):
        message = {
            "type": "unsubscribe",
            "channel": channel,
            "product_ids": [self.coin]
        }
        if channel == 'heartbeats':
            message.pop('product_ids')
        signed_message = self._sign_with_jwt(message)
        await websocket.send(json.dumps(signed_message))

    def _level2(self, data: dict):
        if self.order_book is not None:
            self.order_book.consume_message(data.copy())
        self.db.level2(data)

    async def _consume_messages(self, websocket):
        channel_to_table = defaultdict(lambda: no_op)
        channel_to_table['candles'] = self.db.candles
        channel_to_table['l2_data'] = self._level2
        channel_to_table['market_trades'] = self.db.market_trades
        channel_to_table['ticker'] = self.db.ticker

        async for message in websocket:
            data = json.loads(message)
            
            try:
                channel_to_table[data.get('channel')](data)
            except SchemaError:
                # add logger logic
                continue
            except Invalid:
                # add logger logic
                continue
            except IngressError:
                # add logger logic
                continue

    async def websocket(self):    
        max_message_size = 10 * 1024 * 1024  

        async for websocket in websockets.connect(config.WS_API_URL, max_size=max_message_size):    
            for channel in self.channels:        
                await self._subscribe(websocket, channel)
            
            try:
                consumer_task = asyncio.create_task(
                    self._consume_messages(websocket)
                )
                await consumer_task
            except websockets.ConnectionClosedError:
                continue
            except asyncio.CancelledError:
                break