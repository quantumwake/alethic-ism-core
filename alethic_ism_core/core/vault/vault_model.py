import datetime as dt
from enum import Enum
from typing import Optional

from pydantic import Json, BaseModel


class ConfigType(Enum):
    SECRET = "secret"
    CONFIG_MAP = "config_map"


class Vault(BaseModel):
    id: str
    name: str
    type: str  # e.g., 'kms', 'vault', 'local'
    metadata: Optional[Json] = None  # Additional metadata in JSON format
    created_at: Optional[dt.datetime] = None  # ISO timestamp
    updated_at: Optional[dt.datetime] = None  # ISO timestamp


class Registry(BaseModel):
    id: Optional[str] = None
    name: str
    type: ConfigType
    data: dict  # JSON data stored in the configuration
    vault_key_id: Optional[str] = None  # Encryption key ID
    vault_id: Optional[str] = None  # Reference to Vault
    owner_id: Optional[str] = None  # Multi-tenancy support
    created_at: Optional[dt.datetime] = None  # ISO timestamp
    updated_at: Optional[dt.datetime] = None  # ISO timestamp
    deleted_at: Optional[dt.datetime] = None  # Optional for soft delete
