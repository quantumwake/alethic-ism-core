import datetime as dt
from enum import Enum
from typing import Optional

from pydantic import Json, BaseModel


class ConfigMapType(Enum):
    SECRET = "secret"
    CONFIG_MAP = "config_map"


class VaultType(Enum):
    VAULT = "Vault"
    LOCAL = "Local"
    KMS = "kms"


class Vault(BaseModel):
    id: Optional[str] = None
    name: str
    owner: Optional[str] = None
    # TODO credentials such as api keys or other if we want to support multiple vaults on a per owner basis
    type: VaultType = VaultType.LOCAL  # e.g., 'kms', 'vault', 'local', other provider
    metadata: Optional[Json] = None  # Additional metadata in JSON format
    created_at: Optional[dt.datetime] = None  # ISO timestamp
    updated_at: Optional[dt.datetime] = None  # ISO timestamp


class ConfigMap(BaseModel):
    id: Optional[str] = None
    name: str
    type: ConfigMapType
    data: dict  # JSON data stored in the configuration
    vault_key_id: Optional[str] = None  # Encryption key ID
    vault_id: Optional[str] = None  # Reference to Vault
    owner_id: Optional[str] = None  # Multi-tenancy support
    created_at: Optional[dt.datetime] = None  # ISO timestamp
    updated_at: Optional[dt.datetime] = None  # ISO timestamp
    deleted_at: Optional[dt.datetime] = None  # Optional for soft delete
