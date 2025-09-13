from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field
import json
from typing import Optional


class Settings(BaseSettings):
    # tell pydantic to read from .env file as well as OS env
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

    # --- Temporal ---
    TEMPORAL_HOST: str
    TEMPORAL_NAMESPACE: str
    TEMPORAL_API_KEY: str

    # --- Data API ---
    DATA_API_BASE_URL: str
    DATA_API_DB_KEY: str
    AUTH_STATIC_BEARER_TOKEN: str

    # --- Google Sheets ---
    GOOGLE_SERVICE_ACCOUNT_FILE: Optional[str] = None
    SHEET_ID: str

    # --- SendGrid ---
    SENDGRID_API_KEY: str
    SENDGRID_FROM_EMAIL: str

    # Broker templates
    SENDGRID_BROKER_TEMPLATE_1: str
    SENDGRID_BROKER_TEMPLATE_2: str

    # Client templates
    SENDGRID_CLIENT_TEMPLATE_1: str
    SENDGRID_CLIENT_TEMPLATE_2: str
    SENDGRID_CLIENT_TEMPLATE_3: str

    # Member templates
    SENDGRID_MEMBER_TEMPLATE_1: str
    SENDGRID_MEMBER_TEMPLATE_2: str
    SENDGRID_MEMBER_TEMPLATE_3: str

    # --- Safety override (for testing) ---
    SAFETY_EMAIL_OVERRIDE: Optional[str] = None

    def service_account_info(self):
        if self.GOOGLE_SERVICE_ACCOUNT_FILE:
            with open(self.GOOGLE_SERVICE_ACCOUNT_FILE, "r") as f:
                return json.load(f)
        return None


settings = Settings()
