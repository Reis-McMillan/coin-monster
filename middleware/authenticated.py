import time
import base64
import logging
import httpx
import jwt
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey
from fastapi import Depends, HTTPException, Request
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from types import SimpleNamespace

logger = logging.getLogger("coin-monster.auth")

JWKS_URL = "https://sso.mcmlln.dev/.well-known/jwks.json"

_public_key_cache: Ed25519PublicKey | None = None

security = HTTPBearer()


def _jwk_to_public_key(jwk: dict) -> Ed25519PublicKey:
    # base64url fields omit padding; restore it before decoding
    x = jwk["x"]
    x += "=" * (4 - len(x) % 4)
    raw = base64.urlsafe_b64decode(x)
    return Ed25519PublicKey.from_public_bytes(raw)


async def _get_public_key() -> Ed25519PublicKey:
    global _public_key_cache

    if _public_key_cache is None:
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(JWKS_URL)
                response.raise_for_status()
        except httpx.HTTPError as e:
            raise HTTPException(
                status_code=503,
                detail="Could not fetch authentication keys",
            ) from e

        jwks = response.json()
        jwk = next(
            (k for k in jwks["keys"] if k.get("alg") == "EdDSA"),
            None,
        )
        if jwk is None:
            raise HTTPException(
                status_code=503,
                detail="No EdDSA key found in JWKS",
            )

        _public_key_cache = _jwk_to_public_key(jwk)

    return _public_key_cache


async def authenticate(
    request: Request,
    credentials: HTTPAuthorizationCredentials = Depends(security),
) -> dict:
    jwt_token = credentials.credentials

    try:
        # Verify and decode JWT
        public_key_pem = await _get_public_key()
        decoded = jwt.decode(
            jwt_token,
            public_key_pem,
            algorithms=["EdDSA"]
        )

        # Extract claims
        email = decoded.get('sub')
        roles = decoded.get('roles', [])

        if not email:
            logger.warning("Auth failed: JWT missing 'sub' claim")
            raise HTTPException(status_code=401, detail="Invalid token: missing subject")

        # Store identity in request state for later use
        request.state.auth_cache = SimpleNamespace(email=email, roles=roles)
        logger.info("Authenticated %s via JWT", email)

    except jwt.ExpiredSignatureError:
        logger.warning("Auth failed: JWT token expired")
        raise HTTPException(status_code=401, detail="Token expired")
    except jwt.InvalidTokenError as e:
        logger.warning("Auth failed: invalid JWT token - %s", e)
        raise HTTPException(status_code=401, detail="Invalid token")