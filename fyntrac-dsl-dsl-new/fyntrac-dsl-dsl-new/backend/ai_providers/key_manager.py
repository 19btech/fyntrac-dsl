"""Fernet-based symmetric encryption for API keys at rest."""

import os
import logging
from pathlib import Path
from cryptography.fernet import Fernet

logger = logging.getLogger(__name__)

_KEY_ENV_VAR = "AI_ENCRYPTION_KEY"
_KEY_FILE = Path(__file__).resolve().parent.parent / ".ai_secret_key"

_cached_fernet = None


def _get_fernet() -> Fernet:
    global _cached_fernet
    if _cached_fernet is not None:
        return _cached_fernet

    # 1. Check env var first
    key = os.environ.get(_KEY_ENV_VAR)

    # 2. Check local key file
    if not key and _KEY_FILE.exists():
        try:
            key = _KEY_FILE.read_text().strip()
        except Exception:
            key = None

    # 3. Generate and persist to file
    if not key:
        key = Fernet.generate_key().decode()
        try:
            _KEY_FILE.write_text(key)
            _KEY_FILE.chmod(0o600)
            logger.info("Generated new AI encryption key at %s", _KEY_FILE)
        except Exception as e:
            logger.warning("Could not persist encryption key to file: %s", e)
        os.environ[_KEY_ENV_VAR] = key

    _cached_fernet = Fernet(key.encode() if isinstance(key, str) else key)
    return _cached_fernet


def encrypt_key(plaintext: str) -> str:
    """Encrypt an API key. Returns a base64-encoded token string."""
    return _get_fernet().encrypt(plaintext.encode()).decode()


def decrypt_key(ciphertext: str) -> str:
    """Decrypt an API key token back to plaintext."""
    return _get_fernet().decrypt(ciphertext.encode()).decode()
