import asyncio
import time

import jwt
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
def client(mock_db, mock_jwks, auth_headers):
    app.state.db = mock_db
    app.state.websockets = {}
    with TestClient(app, raise_server_exceptions=False, headers=auth_headers) as client:
        yield client


@pytest.fixture
def unauthed_client(mock_db):
    app.state.db = mock_db
    app.state.websockets = {}
    with TestClient(app, raise_server_exceptions=False) as client:
        yield client
        app.state.websockets.clear()


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


def _mock_task(done=False, cancelled=False, exception=None):
    task = MagicMock(spec=asyncio.Task)
    task.done.return_value = done
    task.cancelled.return_value = cancelled
    task.exception.return_value = exception
    return task


class TestListCoins:

    def test_list_empty(self, unauthed_client):
        response = unauthed_client.get("/coins")

        assert response.status_code == 200
        assert response.json() == []

    def test_list_with_coins(self, unauthed_client):
        app.state.websockets["BTC-USD"] = {"main": _mock_task(), "l2": _mock_task()}
        app.state.websockets["ETH-USD"] = {"main": _mock_task(), "l2": _mock_task()}

        response = unauthed_client.get("/coins")

        assert response.status_code == 200
        assert set(response.json()) == {"BTC-USD", "ETH-USD"}

    def test_list_after_unsubscribe(self, unauthed_client):
        app.state.websockets["ETH-USD"] = {"main": _mock_task(), "l2": _mock_task()}

        response = unauthed_client.get("/coins")

        assert response.status_code == 200
        assert response.json() == ["ETH-USD"]

    def test_list_wrong_method_post(self, unauthed_client):
        response = unauthed_client.post("/coins")
        assert response.status_code == 405

    def test_list_wrong_method_delete(self, unauthed_client):
        response = unauthed_client.delete("/coins")
        assert response.status_code == 405


class TestCoinStatus:

    def test_status_not_found(self, unauthed_client):
        response = unauthed_client.get("/coins/BTC-USD/status")

        assert response.status_code == 404
        assert response.json()["detail"] == "Not subscribed to BTC-USD"

    def test_status_running(self, unauthed_client):
        app.state.websockets["BTC-USD"] = {
            "main": _mock_task(done=False),
            "l2": _mock_task(done=False),
        }

        response = unauthed_client.get("/coins/BTC-USD/status")
        app.state.websockets.clear()

        assert response.status_code == 200
        assert response.json() == {"coin": "BTC-USD", "main": "running", "l2": "running"}

    def test_status_cancelled(self, unauthed_client):
        app.state.websockets["BTC-USD"] = {
            "main": _mock_task(done=True, cancelled=True),
            "l2": _mock_task(done=True, cancelled=True),
        }

        response = unauthed_client.get("/coins/BTC-USD/status")
        app.state.websockets.clear()

        assert response.status_code == 200
        assert response.json() == {"coin": "BTC-USD", "main": "cancelled", "l2": "cancelled"}

    def test_status_failed(self, unauthed_client):
        app.state.websockets["BTC-USD"] = {
            "main": _mock_task(done=True, cancelled=False, exception=RuntimeError("boom")),
            "l2": _mock_task(done=True, cancelled=False, exception=RuntimeError("boom")),
        }

        response = unauthed_client.get("/coins/BTC-USD/status")
        app.state.websockets.clear()

        assert response.status_code == 200
        assert response.json() == {"coin": "BTC-USD", "main": "failed", "l2": "failed"}

    def test_status_done(self, unauthed_client):
        app.state.websockets["BTC-USD"] = {
            "main": _mock_task(done=True, cancelled=False, exception=None),
            "l2": _mock_task(done=True, cancelled=False, exception=None),
        }

        response = unauthed_client.get("/coins/BTC-USD/status")
        app.state.websockets.clear()

        assert response.status_code == 200
        assert response.json() == {"coin": "BTC-USD", "main": "done", "l2": "done"}

    def test_status_mixed(self, unauthed_client):
        app.state.websockets["BTC-USD"] = {
            "main": _mock_task(done=False),
            "l2": _mock_task(done=True, cancelled=False, exception=RuntimeError("boom")),
        }

        response = unauthed_client.get("/coins/BTC-USD/status")
        app.state.websockets.clear()

        assert response.status_code == 200
        assert response.json() == {"coin": "BTC-USD", "main": "running", "l2": "failed"}

    def test_status_wrong_method_post(self, unauthed_client):
        response = unauthed_client.post("/coins/BTC-USD/status")
        assert response.status_code == 405

    def test_status_wrong_method_delete(self, unauthed_client):
        response = unauthed_client.delete("/coins/BTC-USD/status")
        assert response.status_code == 405


class TestAuthentication:

    def test_no_auth_header(self, mock_db, mock_jwks):
        app.state.db = mock_db
        app.state.websockets = {}
        with TestClient(app, raise_server_exceptions=False) as client:
            response = client.post("/coins/TEST-USD")
            assert response.status_code == 401

    def test_expired_token(self, mock_db, mock_jwks, ed25519_keypair):
        private_key, _ = ed25519_keypair
        expired_token = jwt.encode(
            {"sub": "test@example.com", "iat": 1000000, "exp": 1000001},
            private_key,
            algorithm="EdDSA",
        )
        app.state.db = mock_db
        app.state.websockets = {}
        with TestClient(
            app,
            raise_server_exceptions=False,
            headers={"Authorization": f"Bearer {expired_token}"},
        ) as client:
            response = client.post("/coins/TEST-USD")
            assert response.status_code == 401
            assert response.json()["detail"] == "Token expired"

    def test_invalid_token(self, mock_db, mock_jwks):
        app.state.db = mock_db
        app.state.websockets = {}
        with TestClient(
            app,
            raise_server_exceptions=False,
            headers={"Authorization": "Bearer not.a.valid.token"},
        ) as client:
            response = client.post("/coins/TEST-USD")
            assert response.status_code == 401
            assert response.json()["detail"] == "Invalid token"

    def test_missing_subject_claim(self, mock_db, mock_jwks, ed25519_keypair):
        private_key, _ = ed25519_keypair
        now = int(time.time())
        token = jwt.encode(
            {"roles": ["admin"], "iat": now, "exp": now + 3600},
            private_key,
            algorithm="EdDSA",
        )
        app.state.db = mock_db
        app.state.websockets = {}
        with TestClient(
            app,
            raise_server_exceptions=False,
            headers={"Authorization": f"Bearer {token}"},
        ) as client:
            response = client.post("/coins/TEST-USD")
            assert response.status_code == 401
            assert response.json()["detail"] == "Invalid token: missing subject"
