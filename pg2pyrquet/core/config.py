from functools import lru_cache

from pg2pyrquet.core.settings.app import AppSettings


@lru_cache
def get_app_settings() -> AppSettings:
    """
    Return application config.
    """
    return AppSettings()
