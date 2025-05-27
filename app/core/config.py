
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    INFLUXDB_URL: str
    INFLUXDB_TOKEN: str
    INFLUXDB_ORG: str
    INFLUXDB_BUCKET: str

    model_config = SettingsConfigDict(env_file='.env', extra='ignore')

settings = Settings()
