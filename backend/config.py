"""Centralised application configuration loaded from environment variables."""

import os
from pathlib import Path
from dataclasses import dataclass, field
from dotenv import load_dotenv

ROOT_DIR = Path(__file__).parent
load_dotenv(ROOT_DIR / '.env')


@dataclass(frozen=True)
class Settings:
    mongo_url: str = os.environ.get('MONGO_URL', 'mongodb://localhost:27017')
    db_name: str = os.environ.get('DB_NAME', 'dsl_db')
    mongo_timeout_ms: int = int(os.environ.get('MONGO_TIMEOUT_MS', '5000'))
    cors_origins: list[str] = field(
        default_factory=lambda: os.environ.get('CORS_ORIGINS', 'http://localhost:3000').split(',')
    )
    host: str = os.environ.get('HOST', '0.0.0.0')
    port: int = int(os.environ.get('PORT', '8000'))
    log_level: str = os.environ.get('LOG_LEVEL', 'INFO')
    zitadel_issuer_uri: str = os.environ.get('ZITADEL_ISSUER_URI', '')
    zitadel_project_id: str = os.environ.get('ZITADEL_PROJECT_ID', '')
    dataloader_base_uri: str = os.environ.get('DATALOADER_BASE_URI', 'http://localhost:8585/api/dataloader')


settings = Settings()
