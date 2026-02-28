import base64
import time

import jwt
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey


@pytest.fixture(scope="session")
def ed25519_keypair():
    """Generate an Ed25519 key pair for JWT signing/verification."""
    private_key = Ed25519PrivateKey.generate()
    public_key = private_key.public_key()
    return private_key, public_key


@pytest.fixture
def auth_token(ed25519_keypair):
    """Create a signed EdDSA JWT token with standard claims."""
    private_key, _ = ed25519_keypair
    now = int(time.time())
    payload = {
        "sub": "test@example.com",
        "roles": ["admin"],
        "iat": now,
        "exp": now + 3600,
    }
    return jwt.encode(payload, private_key, algorithm="EdDSA")


@pytest.fixture
def auth_headers(auth_token):
    """Return Authorization headers with a valid Bearer token."""
    return {"Authorization": f"Bearer {auth_token}"}


@pytest.fixture
def mock_jwks(ed25519_keypair):
    """Mock the JWKS endpoint to return the test Ed25519 public key."""
    import middleware.authenticated as auth_mod

    _, public_key = ed25519_keypair
    raw_bytes = public_key.public_bytes_raw()
    x = base64.urlsafe_b64encode(raw_bytes).rstrip(b"=").decode()

    jwks_response = {
        "keys": [{
            "kty": "OKP",
            "crv": "Ed25519",
            "alg": "EdDSA",
            "x": x,
        }]
    }

    mock_response = MagicMock()
    mock_response.json.return_value = jwks_response
    mock_response.raise_for_status = MagicMock()

    auth_mod._public_key_cache = None

    with patch("middleware.authenticated.httpx.AsyncClient") as mock_client_cls:
        mock_instance = AsyncMock()
        mock_client_cls.return_value.__aenter__ = AsyncMock(return_value=mock_instance)
        mock_client_cls.return_value.__aexit__ = AsyncMock(return_value=False)
        mock_instance.get = AsyncMock(return_value=mock_response)
        yield

    auth_mod._public_key_cache = None
