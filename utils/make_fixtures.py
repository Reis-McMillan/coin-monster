import asyncio
import json
import websockets

from config import config
from modules.websocket import Websocket


def write_fixtures(fixtures: dict):
    for k, v in fixtures.items():
        with open(f'tests/fixtures/{k}.json', 'w') as f:
            json.dump(v, f)


class FixtureWebsocket(Websocket):
    """Websocket subclass for collecting fixture data instead of writing to database."""

    def __init__(self, coins: list[str], channels: list[str], timeout_seconds: int = 60):
        # Use first coin for parent init, but store all coins for fixture collection
        super().__init__(coins[0], channels)
        self.coins = coins
        self.timeout_seconds = timeout_seconds
        self.fixtures = {'websocket': []}

    async def _subscribe(self, websocket, channel):
        message = {
            "type": "subscribe",
            "channel": channel,
            "product_ids": self.coins
        }
        if channel == 'heartbeats':
            message.pop('product_ids')
        signed_message = self._sign_with_jwt(message)
        await websocket.send(json.dumps(signed_message))

    async def _unsubscribe(self, websocket, channel):
        message = {
            "type": "unsubscribe",
            "channel": channel,
            "product_ids": self.coins
        }
        if channel == 'heartbeats':
            message.pop('product_ids')
        signed_message = self._sign_with_jwt(message)
        await websocket.send(json.dumps(signed_message))

    async def _consume_messages(self, websocket):
        try:
            async for message in websocket:
                data = json.loads(message)

                self.fixtures['websocket'].append(data)
                channel = data.get('channel')
                if channel:
                    if channel in self.fixtures:
                        self.fixtures[channel].append(data)
                    else:
                        self.fixtures[channel] = [data]
        except asyncio.CancelledError:
            pass

    async def make_fixtures(self):
        max_message_size = 10 * 1024 * 1024

        try:
            async with websockets.connect(config.WS_API_URL, max_size=max_message_size) as websocket:
                for channel in self.channels:
                    await self._subscribe(websocket, channel)

                consumer_task = asyncio.create_task(self._consume_messages(websocket))

                try:
                    await asyncio.wait_for(asyncio.shield(consumer_task), timeout=self.timeout_seconds)
                except asyncio.TimeoutError:
                    consumer_task.cancel()
                    await consumer_task
                    write_fixtures(self.fixtures)
                except Exception as e:
                    print(f"Unexpected error occurred: {e}")
                finally:
                    for channel in self.channels:
                        await self._unsubscribe(websocket, channel)

        except websockets.exceptions.ConnectionClosed as e:
            print(f"Connection closed: {e}")
