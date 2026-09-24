from typing import Literal
from pydantic import Field, SecretStr, EmailStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    worker_lane: Literal["bulk", "urgent"] = "bulk"
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")
    database_url: str
    rabbitmq_url: SecretStr
    auth_url: str
    auth_reader_token: SecretStr
    service_tokens: dict[str, SecretStr]
    smtp_host: str
    smtp_port: int = 1025
    smtp_from: EmailStr = "cinema@example.org"
    smtp_username: str | None = None
    smtp_password: SecretStr | None = None
    smtp_start_tls: bool = False
    smtp_use_tls: bool = False
    concurrency: int = Field(default=5, ge=1, le=100)
    shutdown_timeout_seconds: float = Field(default=30, ge=1, le=300)
    urgent_delivery_ttl_seconds: int = Field(default=3600, ge=60, le=604800)
    bulk_delivery_ttl_seconds: int = Field(default=86400, ge=60, le=2592000)
    outbox_retention_seconds: int = Field(default=604800, ge=3600, le=31536000)

    @model_validator(mode="after")
    def tokens_are_distinct(self):
        values = [value.get_secret_value() for value in self.service_tokens.values()]
        if set(self.service_tokens) != {"auth", "catalog", "operator"}:
            raise ValueError("service_tokens requires auth, catalog, operator")
        if any(len(value) < 24 for value in values) or len(set(values)) != 3:
            raise ValueError("service tokens must be distinct and at least 24 characters")
        if len(self.auth_reader_token.get_secret_value()) < 24:
            raise ValueError("auth reader token must be at least 24 characters")
        if self.smtp_start_tls and self.smtp_use_tls:
            raise ValueError("choose STARTTLS or implicit TLS")
        return self
