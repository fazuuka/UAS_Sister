from pydantic_settings import BaseSettings
from functools import lru_cache


class Settings(BaseSettings):
    database_url: str = "postgresql+asyncpg://user:pass@storage:5432/logdb"
    redis_url: str = "redis://broker:6379"
    worker_count: int = 4
    redis_stream_key: str = "events:stream"
    redis_consumer_group: str = "aggregator-group"
    log_level: str = "INFO"

    class Config:
        env_file = ".env"


@lru_cache()
def get_settings() -> Settings:
    return Settings()
