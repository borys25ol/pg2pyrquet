import logging

from pydantic_settings import BaseSettings


class AppSettings(BaseSettings):
    """
    Base application settings
    """

    logging_level: int = logging.INFO

    # Postgres env variables.
    postgres_host: str
    postgres_port: int
    postgres_user: str
    postgres_password: str

    class Config:
        validate_assignment = True
        env_file = ".env"
