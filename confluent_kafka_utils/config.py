# config.py
from pathlib import Path
from typing import Optional
from pydantic import Field, validator
from pydantic_settings import BaseSettings
import yaml
from dotenv import load_dotenv
import os

# Load .env if present
load_dotenv()

# Try to load yaml defaults (optional)
_yaml_path = Path("confluent_kafka_utils_config.yaml")
_yaml_config = {}
if _yaml_path.exists():
    with _yaml_path.open() as f:
        _yaml_config = yaml.safe_load(f) or {}

class Settings(BaseSettings):
    # bootstrap_servers: str = Field(..., env="BOOTSTRAP_SERVERS")
    bootstrap_servers: str = Field(None, env="BOOTSTRAP_SERVERS")
    schema_registry_url: Optional[str] = Field(None, env="SCHEMA_REGISTRY_URL")
    topic: str = Field(None, env="TOPIC")
    group_id: Optional[str] = Field(None, env="GROUP_ID")
    # use_schema_registry: bool = Field(False, env="USE_SCHEMA_REGISTRY")
    key_avsc_path: Optional[str] = Field(None, env="KEY_AVSC_PATH")
    value_avsc_path: Optional[str] = Field(None, env="VALUE_AVSC_PATH")

    class Config:
        # values are loaded from env first; we will merge yaml defaults below
        env_file = ".env"
        case_sensitive = False

    @validator("bootstrap_servers", pre=True, always=True)
    def set_from_yaml_if_missing(cls, v):
        return v or _yaml_config.get("bootstrap_servers")

    @validator("schema_registry_url", pre=True, always=True)
    def set_sr_from_yaml(cls, v):
        return v or _yaml_config.get("schema_registry_url")

    @validator("topic", pre=True, always=True)
    def set_topic_from_yaml(cls, v):
        return v or _yaml_config.get("topic")

    @validator("group_id", pre=True, always=True)
    def set_group_id_from_yaml(cls, v):
        return v or _yaml_config.get("group_id")

    @validator("key_avsc_path", pre=True, always=True)
    def set_key_avsc_path_from_yaml(cls, v):
        return v or _yaml_config.get("key_avsc_path")

    @validator("value_avsc_path", pre=True, always=True)
    def set_value_avsc_path_from_yaml(cls, v):
        return v or _yaml_config.get("value_avsc_path")

# single shared settings instance
settings = Settings()
