import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from fastapi.testclient import TestClient

from app import app


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
def client(mock_db):
    app.state.db = mock_db
    app.state.websockets = {}
    with TestClient(app, raise_server_exceptions=False) as client:
        yield client


class TestSubscribeCoin:

    @patch('app.Websocket')
    def test_subscribe_success(self, mock_ws_class, client):
        mock_ws_instance = MagicMock()
        mock_ws_instance.websocket = AsyncMock()
        mock_ws_class.return_value = mock_ws_instance

        response = client.post("/coins/BTC-USD")

        assert response.status_code == 201
        assert response.json() == {
            "coin": "BTC-USD",
            "message": "Subscribed to BTC-USD"
        }
        assert "BTC-USD" in app.state.websockets

    @patch('app.Websocket')
    def test_subscribe_creates_two_tasks(self, mock_ws_class, client):
        mock_ws_instance = MagicMock()
        mock_ws_instance.websocket = AsyncMock()
        mock_ws_class.return_value = mock_ws_instance

        client.post("/coins/BTC-USD")

        tasks = app.state.websockets["BTC-USD"]
        assert "main" in tasks
        assert "l2" in tasks

    @patch('app.Websocket')
    def test_subscribe_conflict(self, mock_ws_class, client):
        mock_ws_instance = MagicMock()
        mock_ws_instance.websocket = AsyncMock()
        mock_ws_class.return_value = mock_ws_instance

        client.post("/coins/ETH-USD")
        response = client.post("/coins/ETH-USD")

        assert response.status_code == 409
        assert response.json()["detail"] == "Already subscribed to ETH-USD"

    def test_subscribe_wrong_method_get(self, client):
        response = client.get("/coins/BTC-USD")
        assert response.status_code == 405

    def test_subscribe_wrong_method_put(self, client):
        response = client.put("/coins/BTC-USD")
        assert response.status_code == 405

    def test_subscribe_wrong_method_patch(self, client):
        response = client.patch("/coins/BTC-USD")
        assert response.status_code == 405


class TestUnsubscribeCoin:

    @patch('app.Websocket')
    def test_unsubscribe_success(self, mock_ws_class, client):
        mock_ws_instance = MagicMock()
        mock_ws_instance.websocket = AsyncMock()
        mock_ws_class.return_value = mock_ws_instance

        client.post("/coins/BTC-USD")
        response = client.delete("/coins/BTC-USD")

        assert response.status_code == 200
        assert response.json() == {
            "coin": "BTC-USD",
            "message": "Unsubscribed from BTC-USD"
        }
        assert "BTC-USD" not in app.state.websockets

    def test_unsubscribe_not_found(self, client):
        response = client.delete("/coins/DOGE-USD")

        assert response.status_code == 404
        assert response.json()["detail"] == "Not subscribed to DOGE-USD"

    @patch('app.Websocket')
    def test_unsubscribe_twice(self, mock_ws_class, client):
        mock_ws_instance = MagicMock()
        mock_ws_instance.websocket = AsyncMock()
        mock_ws_class.return_value = mock_ws_instance

        client.post("/coins/BTC-USD")
        client.delete("/coins/BTC-USD")
        response = client.delete("/coins/BTC-USD")

        assert response.status_code == 404
        assert response.json()["detail"] == "Not subscribed to BTC-USD"


class TestListCoins:

    def test_list_empty(self, client):
        response = client.get("/coins")

        assert response.status_code == 200
        assert response.json() == []

    @patch('app.Websocket')
    def test_list_with_coins(self, mock_ws_class, client):
        mock_ws_instance = MagicMock()
        mock_ws_instance.websocket = AsyncMock()
        mock_ws_class.return_value = mock_ws_instance

        client.post("/coins/BTC-USD")
        client.post("/coins/ETH-USD")

        response = client.get("/coins")

        assert response.status_code == 200
        assert set(response.json()) == {"BTC-USD", "ETH-USD"}

    @patch('app.Websocket')
    def test_list_after_unsubscribe(self, mock_ws_class, client):
        mock_ws_instance = MagicMock()
        mock_ws_instance.websocket = AsyncMock()
        mock_ws_class.return_value = mock_ws_instance

        client.post("/coins/BTC-USD")
        client.post("/coins/ETH-USD")
        client.delete("/coins/BTC-USD")

        response = client.get("/coins")

        assert response.status_code == 200
        assert response.json() == ["ETH-USD"]

    def test_list_wrong_method_post(self, client):
        response = client.post("/coins")
        assert response.status_code == 405

    def test_list_wrong_method_delete(self, client):
        response = client.delete("/coins")
        assert response.status_code == 405
