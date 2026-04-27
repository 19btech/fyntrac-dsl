"""
ZITADEL JWT Bearer authentication dependency for FastAPI.
Adapted for fyntrac-dsl.
"""

import time
import logging
import os
from typing import Optional

import httpx
from fastapi import Depends, HTTPException, status, Request
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from jose import JWTError, jwt

from .config import settings

logger = logging.getLogger(__name__)

# ── JWKS cache ───────────────────────────────────────────────────────────
_jwks_cache: Optional[dict] = None
_jwks_cache_expiry: float = 0.0
_JWKS_CACHE_TTL_SECONDS: int = 3600  # 1 hour

# Set auto_error=False to allow us to log/handle missing tokens manually
security_scheme = HTTPBearer(auto_error=False)


async def _fetch_openid_configuration(issuer_uri: str) -> dict:
    """Fetch the OpenID Connect discovery document."""
    url = f"{issuer_uri.rstrip('/')}/.well-known/openid-configuration"
    async with httpx.AsyncClient() as client:
        response = await client.get(url, timeout=10.0)
        response.raise_for_status()
        return response.json()


async def _fetch_jwks(jwks_uri: str) -> dict:
    """Fetch the JSON Web Key Set from the JWKS URI."""
    async with httpx.AsyncClient() as client:
        response = await client.get(jwks_uri, timeout=10.0)
        response.raise_for_status()
        return response.json()


async def _get_jwks(issuer_uri: str) -> dict:
    """Return cached JWKS keys, refreshing if expired."""
    global _jwks_cache, _jwks_cache_expiry

    if _jwks_cache and time.time() < _jwks_cache_expiry:
        return _jwks_cache

    logger.info("Refreshing JWKS keys from %s", issuer_uri)
    try:
        oidc_config = await _fetch_openid_configuration(issuer_uri)
        jwks_uri = oidc_config["jwks_uri"]
        _jwks_cache = await _fetch_jwks(jwks_uri)
        _jwks_cache_expiry = time.time() + _JWKS_CACHE_TTL_SECONDS
        return _jwks_cache
    except Exception as e:
        logger.error("Failed to refresh JWKS: %s", str(e))
        raise


async def verify_jwt(
    request: Request,
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(security_scheme),
) -> dict:
    """
    FastAPI dependency that validates the JWT Bearer token.
    """
    # For development, allow skipping auth via environment variable
    if os.environ.get("SKIP_AUTH") == "true":
        return {"sub": "dev_user", "email": "dev@example.com", "tenant": "master"}

    if not credentials:
        # Log headers to help diagnose why the token is missing
        logger.warning("Missing Authorization header. Headers received: %s", dict(request.headers))
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing Authorization header",
            headers={"WWW-Authenticate": "Bearer"},
        )

    token = credentials.credentials
    
    if not settings.zitadel_issuer_uri:
        logger.warning("ZITADEL_ISSUER_URI is not configured, allowing request in insecure mode")
        return {"sub": "anonymous"}

    try:
        jwks = await _get_jwks(settings.zitadel_issuer_uri)

        # Decode the token header to find the key ID
        unverified_header = jwt.get_unverified_header(token)
        kid = unverified_header.get("kid")

        # Find the matching public key
        rsa_key = {}
        for key in jwks.get("keys", []):
            if key.get("kid") == kid:
                rsa_key = key
                break

        if not rsa_key:
            logger.error("No matching signing key found for kid: %s", kid)
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Unable to find matching signing key",
                headers={"WWW-Authenticate": "Bearer"},
            )

        # Build audience list — ZITADEL project ID
        audience = settings.zitadel_project_id

        # Decode and validate the token
        payload = jwt.decode(
            token,
            rsa_key,
            algorithms=["RS256"],
            issuer=settings.zitadel_issuer_uri,
            audience=audience,
            options={
                "verify_aud": bool(audience),
                "verify_iss": True,
                "verify_exp": True,
                "verify_at_hash": False,   # ID token only — no access token to verify at_hash
            },
        )

        logger.info("Successfully verified JWT for subject: %s", payload.get("sub"))
        return payload

    except JWTError as e:
        logger.warning("JWT validation failed: %s", str(e))
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=f"Token validation failed: {str(e)}",
            headers={"WWW-Authenticate": "Bearer"},
        )
    except Exception as e:
        logger.error("Internal error during JWT validation: %s", str(e))
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Unable to validate token",
        )
