from pydantic_settings import BaseSettings, SettingsConfigDict


class AppSettings(BaseSettings):
    """
    Base application settings
    """

    model_config = SettingsConfigDict(
        env_file=".env", validate_assignment=True
    )

    # Postgres env variables.
    postgres_host: str
    postgres_port: int
    postgres_user: str
    postgres_password: str
