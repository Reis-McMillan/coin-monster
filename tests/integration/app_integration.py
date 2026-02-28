import time
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from fastapi.testclient import TestClient

from app import app
from db import Base


@pytest.fixture
def mock_db():
    db = MagicMock()
    db.candles = MagicMock()
    db.level2 = MagicMock()
    db.market_trades = MagicMock()
    db.order_book = MagicMock()
    db.ticker = MagicMock()
    return db


@pytest.fixture
def client(mock_db, mock_jwks, auth_headers):
    with patch('app.initialize_db', new_callable=AsyncMock, return_value=mock_db), \
         patch.object(Base, 'establish'), \
         patch.object(Base, 'close', new_callable=AsyncMock):
        app.state.db = mock_db
        app.state.websockets = {}
        with TestClient(app, raise_server_exceptions=False, headers=auth_headers) as client:
            yield client


class TestAppIntegration:

    def test_subscribe_creates_live_websocket_tasks(self, client):
        response = client.post("/coins/BTC-USD")

        assert response.status_code == 201

        tasks = app.state.websockets.get("BTC-USD")
        assert tasks is not None
        assert "main" in tasks
        assert "l2" in tasks
        assert not tasks["main"].done()
        assert not tasks["l2"].done()

        # cleanup
        client.delete("/coins/BTC-USD")

    def test_subscribe_receives_real_data(self, mock_db, client):
        client.post("/coins/BTC-USD")

        # wait for real data to arrive from Coinbase
        for _ in range(40):
            if (mock_db.candles.call_count > 0
                    or mock_db.ticker.call_count > 0
                    or mock_db.market_trades.call_count > 0
                    or mock_db.level2.call_count > 0):
                break
            time.sleep(0.5)

        total_calls = (mock_db.candles.call_count
                       + mock_db.ticker.call_count
                       + mock_db.market_trades.call_count
                       + mock_db.level2.call_count)
        assert total_calls > 0, "No real data received from Coinbase"

        # cleanup
        client.delete("/coins/BTC-USD")

    def test_unsubscribe_stops_live_tasks(self, client):
        client.post("/coins/BTC-USD")

        tasks = app.state.websockets["BTC-USD"]
        main_task = tasks["main"]
        l2_task = tasks["l2"]

        response = client.delete("/coins/BTC-USD")

        assert response.status_code == 200
        assert "BTC-USD" not in app.state.websockets
        assert main_task.done()
        assert l2_task.done()

    def test_multiple_coins_live(self, client):
        client.post("/coins/BTC-USD")
        client.post("/coins/ETH-USD")

        assert "BTC-USD" in app.state.websockets
        assert "ETH-USD" in app.state.websockets

        btc_tasks = app.state.websockets["BTC-USD"]
        eth_tasks = app.state.websockets["ETH-USD"]

        client.delete("/coins/BTC-USD")

        assert "BTC-USD" not in app.state.websockets
        assert btc_tasks["main"].done()
        assert btc_tasks["l2"].done()

        # ETH tasks should still be active
        assert "ETH-USD" in app.state.websockets
        assert not eth_tasks["main"].done()
        assert not eth_tasks["l2"].done()

        # cleanup
        client.delete("/coins/ETH-USD")

    def test_full_lifecycle_with_live_data(self, mock_db, client):
        # subscribe
        response = client.post("/coins/BTC-USD")
        assert response.status_code == 201

        # wait for real data
        for _ in range(40):
            if (mock_db.candles.call_count > 0
                    or mock_db.ticker.call_count > 0
                    or mock_db.market_trades.call_count > 0
                    or mock_db.level2.call_count > 0):
                break
            time.sleep(0.5)

        # list coins
        response = client.get("/coins")
        assert response.status_code == 200
        assert "BTC-USD" in response.json()

        # unsubscribe
        response = client.delete("/coins/BTC-USD")
        assert response.status_code == 200

        # list empty
        response = client.get("/coins")
        assert response.status_code == 200
        assert response.json() == []

    def test_status_running_after_subscribe(self, client):
        client.post("/coins/BTC-USD")

        response = client.get("/coins/BTC-USD/status")

        assert response.status_code == 200
        body = response.json()
        assert body["coin"] == "BTC-USD"
        assert body["main"] == "running"
        assert body["l2"] == "running"

        client.delete("/coins/BTC-USD")

    def test_status_not_found_after_unsubscribe(self, client):
        client.post("/coins/BTC-USD")
        client.delete("/coins/BTC-USD")

        response = client.get("/coins/BTC-USD/status")

        assert response.status_code == 404

    def test_lifespan_shutdown(self, mock_db, mock_jwks, auth_headers):
        with patch('app.initialize_db', new_callable=AsyncMock, return_value=mock_db), \
             patch.object(Base, 'establish'), \
             patch.object(Base, 'close', new_callable=AsyncMock):
            with TestClient(app, raise_server_exceptions=False, headers=auth_headers) as client:
                client.post("/coins/BTC-USD")
                client.post("/coins/ETH-USD")

                btc_tasks = app.state.websockets["BTC-USD"]
                eth_tasks = app.state.websockets["ETH-USD"]

            # exiting the context triggers lifespan shutdown
            assert btc_tasks["main"].done()
            assert btc_tasks["l2"].done()
            assert eth_tasks["main"].done()
            assert eth_tasks["l2"].done()
