import asyncio
import json
import pytest
import pytest_asyncio
import jwt
from unittest.mock import MagicMock, patch, AsyncMock, call
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives import serialization
from voluptuous.error import Invalid, MultipleInvalid
from websockets.exceptions import ConnectionClosedError
from websockets.frames import Close

from modules.websocket import Websocket
from modules.order_book import OrderBook
from tests.fixtures.websocket import websocket_all

@pytest.fixture(scope="class")
def test_keys():
    """Generates a reusable ECDSA key pair for the class."""
    private_key = ec.generate_private_key(ec.SECP256R1())
    pem = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    ).decode('utf-8')
    return pem

@pytest.fixture
def mock_db():
    db = MagicMock()
    OrderBook.set_order_book_db(db.order_book)
    return db

@pytest.fixture
def sut(mock_db):
    Websocket.set_db(mock_db)
    return Websocket(coin='BTC-USD', channels=['heartbeats', 'candles', 'l2_data', 'market_trades', 'ticker'])

@pytest.mark.asyncio
class TestWebsocket:

    @patch('modules.websocket.secrets.token_hex',
           return_value='74b1ba49ba76710931283aa30498494f40a5e315e26e9e47bd0f9254a03c7a75')
    @patch('modules.websocket.config')
    @patch('modules.websocket.time.time')
    async def test_build_jwt(self, mock_time, mock_config, mock_nonce, test_keys, mock_db):
        mock_time.return_value = 1700000000
        mock_config.KEY_SECRET = test_keys
        mock_config.KEY_NAME = "test-kid"

        with patch.object(Websocket, 'PRIVATE_KEY', serialization.load_pem_private_key(
            test_keys.encode('utf-8'), password=None
        )):
            Websocket.set_db(mock_db)
            ws_instance = Websocket(coin='BTC-USD', channels=['ticker'])
            token = ws_instance._build_jwt()

        payload = jwt.decode(token, options={"verify_signature": False})
        header = jwt.get_unverified_header(token)

        assert payload['sub'] == "test-kid"
        assert payload['iss'] == "cdp"
        assert payload['nbf'] == 1700000000
        assert payload['exp'] == 1700000000 + 120
        assert header['kid'] == "test-kid"
        assert header['alg'] == 'ES256'
        assert header['nonce'] == mock_nonce.return_value

    async def test_sign_with_jwt(self, sut):
        with patch.object(sut, '_build_jwt', return_value='mock-token'):
            message = {
                "type": "subscribe",
                "channel": "ticker",
                "product_ids": ['BTC-USD']
            }
            message = sut._sign_with_jwt(message)
            assert message['jwt'] == 'mock-token'

    async def test_subscribe_success(self, sut):
        with patch.object(sut, '_build_jwt', return_value='mock-token'):
            mock_ws = MagicMock()
            mock_ws.send = AsyncMock()

            # Test standard channel
            await sut._subscribe(mock_ws, "ticker")

            expected_call_args = call(json.dumps({
                "type": "subscribe",
                "channel": "ticker",
                "product_ids": [sut.coin],
                "jwt": 'mock-token'
            }))

            assert mock_ws.send.call_args == expected_call_args

            # Test heartbeats channel (no product_ids)
            await sut._subscribe(mock_ws, "heartbeats")

            expected_call_args = call(json.dumps({
                "type": "subscribe",
                "channel": "heartbeats",
                "jwt": 'mock-token'
            }))

            assert mock_ws.send.call_args == expected_call_args

    async def test_subscribe_fail(self, sut):
        with patch.object(sut, '_build_jwt', return_value='mock-token'):
            mock_ws = MagicMock()
            close_info = Close(code=1006, reason="Internal Server Error")
            mock_ws.send = AsyncMock(side_effect=ConnectionClosedError(rcvd=close_info, sent=None))

            with pytest.raises(ConnectionClosedError):
                await sut._subscribe(mock_ws, 'candles')

    async def test_unsubscribe_success(self, sut):
        with patch.object(sut, '_build_jwt', return_value='mock-token'):
            mock_ws = MagicMock()
            mock_ws.send = AsyncMock()

            # Test standard channel
            await sut._unsubscribe(mock_ws, "ticker")

            expected_call_args = call(json.dumps({
                "type": "unsubscribe",
                "channel": "ticker",
                "product_ids": [sut.coin],
                "jwt": 'mock-token'
            }))

            assert mock_ws.send.call_args == expected_call_args

            # Test heartbeats channel (no product_ids)
            await sut._unsubscribe(mock_ws, "heartbeats")

            expected_call_args = call(json.dumps({
                "type": "unsubscribe",
                "channel": "heartbeats",
                "jwt": 'mock-token'
            }))

            assert mock_ws.send.call_args == expected_call_args

    async def test_unsubscribe_fail(self, sut):
        with patch.object(sut, '_build_jwt', return_value='mock-token'):
            mock_ws = MagicMock()
            close_info = Close(code=1006, reason="Internal Server Error")
            mock_ws.send = AsyncMock(side_effect=ConnectionClosedError(rcvd=close_info, sent=None))

            with pytest.raises(ConnectionClosedError):
                await sut._unsubscribe(mock_ws, 'candles')

    async def test_consume_messages(self, sut, mock_db):
        messages = [json.dumps(m) for m in websocket_all]
        mock_ws = AsyncMock()
        mock_ws.__aiter__.return_value = messages

        await sut._consume_messages(mock_ws)

        assert mock_db.candles.call_count != 0
        assert mock_db.order_book.call_count != 0
        assert mock_db.level2.call_count != 0
        assert mock_db.market_trades.call_count != 0
        assert mock_db.ticker.call_count != 0

    async def test_consume_messages_errors(self, mock_db):
        from questdb.ingress import IngressError
        from pandera.errors import SchemaError

        mock_db.candles = MagicMock(side_effect=IngressError(420, "kirked!"))
        mock_db.level2 = MagicMock(side_effect=SchemaError(schema=None, data=None, message="kirked"))
        mock_db.ticker = MagicMock(side_effect=MultipleInvalid([Invalid("test error")]))

        Websocket.set_db(mock_db)
        ws_instance = Websocket(coin='BTC-USD', channels=['heartbeats', 'candles', 'level2', 'market_trades', 'ticker'])

        messages = [json.dumps(m) for m in websocket_all]
        mock_ws = AsyncMock()
        mock_ws.__aiter__.return_value = messages

        try:
            await ws_instance._consume_messages(mock_ws)
        except Exception as e:
            pytest.fail(f"Received error: {e}")

    async def test_consume_messages_fail(self, sut):
        mock_ws = AsyncMock()
        close_info = Close(code=1006, reason="Internal Server Error")
        mock_ws.__aiter__.side_effect = ConnectionClosedError(rcvd=close_info, sent=None)

        try:
            await sut._consume_messages(mock_ws)
            pytest.fail("Test expected method to fail.")
        except ConnectionClosedError:
            pass

    @patch("modules.websocket.websockets.connect")
    async def test_websocket_success(self, mock_connect, sut, mock_db):
        messages = [json.dumps(m) for m in websocket_all]
        mock_ws = AsyncMock()
        mock_ws.__aiter__.return_value = messages

        mock_connect.return_value.__aiter__.return_value = [mock_ws]

        with patch.object(sut, '_build_jwt', return_value='mock-token'):
            await asyncio.wait_for(
                sut.websocket(),
                timeout=2.0
            )

        assert mock_db.candles.call_count != 0
        assert mock_db.level2.call_count != 0
        assert mock_db.market_trades.call_count != 0
        assert mock_db.ticker.call_count != 0

    @patch("modules.websocket.websockets.connect")
    async def test_websocket_errors(self, mock_connect, mock_db):
        from questdb.ingress import IngressError
        from pandera.errors import SchemaError

        mock_db.candles = MagicMock(side_effect=IngressError(420, "kirked!"))
        mock_db.level2 = MagicMock(side_effect=SchemaError(schema=None, data=None, message="kirked"))
        mock_db.ticker = MagicMock(side_effect=MultipleInvalid([Invalid("test error")]))

        Websocket.set_db(mock_db)
        ws_instance = Websocket(coin='BTC-USD', channels=['heartbeats', 'candles', 'level2', 'market_trades', 'ticker'])

        messages = [json.dumps(m) for m in websocket_all]
        mock_ws = AsyncMock()
        mock_ws.__aiter__.return_value = messages

        mock_connect.return_value.__aiter__.return_value = [mock_ws]

        try:
            with patch.object(ws_instance, '_build_jwt', return_value='mock-token'):
                await ws_instance.websocket()
        except Exception as e:
            pytest.fail(f"Test did not expect to receive error: {e}")

    @patch("modules.websocket.websockets.connect")
    async def test_websocket_connection_error(self, mock_connect, sut, mock_db):
        messages = [json.dumps(m) for m in websocket_all]
        mock_ws0 = AsyncMock()
        close_info = Close(code=1006, reason="Internal Server Error")
        mock_ws0.__aiter__.side_effect = ConnectionClosedError(rcvd=close_info, sent=None)
        mock_ws1 = AsyncMock()
        mock_ws1.__aiter__.return_value = messages

        mock_connect.return_value.__aiter__.return_value = [mock_ws0, mock_ws1]

        try:
            with patch.object(sut, '_build_jwt', return_value='mock-token'):
                await sut.websocket()
        except Exception as e:
            pytest.fail(f"Test did not expect to receive error: {e}")
            assert mock_db.candles.call_count != 0

    @patch("modules.websocket.websockets.connect")
    async def test_websocket_cancel(self, mock_connect, sut, mock_db):
        messages = [json.dumps(m) for m in websocket_all]
        mock_ws0 = AsyncMock()
        mock_ws0.__aiter__.side_effect = asyncio.CancelledError()
        mock_ws1 = AsyncMock()
        mock_ws1.__aiter__.return_value = messages

        mock_connect.return_value.__aiter__.return_value = [mock_ws0, mock_ws1]

        try:
            with patch.object(sut, '_build_jwt', return_value='mock-token'):
                await sut.websocket()
        except Exception as e:
            pytest.fail(f"Test did not expect to receive error: {e}")
            assert mock_db.candles.call_count == 0
