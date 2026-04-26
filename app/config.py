from urllib.parse import quote_plus
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        env_prefix=""  # garante sem prefixo
    )

    database_url: str = ""
    db_host: str = "localhost"
    db_port: int = 3306
    db_user: str = "root"
    db_password: str = "password"
    db_name: str = "softwave"
    db_connection_timeout: int = 15
    db_autocommit: bool = False
    db_charset: str = "utf8mb4"
    db_use_pure: bool = True
    api_title: str = "ETL Extratos Bancários"
    api_version: str = "1.0.0"

    @property
    def resolved_database_url(self) -> str:
        if self.database_url.strip():
            return self.database_url.strip()

        # Encode da senha para escapar caracteres especiais
        password_encoded = quote_plus(self.db_password)

        return (
            f"mysql+mysqlconnector://{self.db_user}:{password_encoded}"
            f"@{self.db_host}:{self.db_port}/{self.db_name}"
            f"?charset={self.db_charset}&connection_timeout={self.db_connection_timeout}"
        )


settings = Settings()