"""
Data models for bundle creation and publishing.

These Pydantic models provide type safety and validation for the bundle
publishing workflow, from parsing modelops.yaml to creating OCI manifests.
"""
from __future__ import annotations

import hashlib
import json
from enum import Enum
from pathlib import Path
from typing import Dict, List, Literal, Optional, Union

from pydantic import BaseModel, Field, field_validator, computed_field


class StorageTier(str, Enum):
    """External storage access tiers."""
    HOT = "hot"
    COOL = "cool" 
    ARCHIVE = "archive"


class ExternalRule(BaseModel):
    """Rule for classifying files as external storage."""
    pattern: str = Field(..., description="Glob pattern for matching files")
    uri_template: str = Field(..., description="URI template with {path} placeholder")
    tier: StorageTier = Field(default=StorageTier.HOT, description="Storage access tier")
    size_threshold: Optional[int] = Field(default=None, description="Minimum file size in bytes")
    
    def matches(self, path: str, size: int) -> bool:
        """Check if this rule matches the given file."""
        from fnmatch import fnmatch
        
        # Check pattern match
        if not fnmatch(path, self.pattern):
            return False
        
        # Check size threshold if specified
        if self.size_threshold is not None and size < self.size_threshold:
            return False
        
        return True
    
    def format_uri(self, path: str) -> str:
        """Format URI for the given path."""
        return self.uri_template.format(path=path)


class LayerSpec(BaseModel):
    """Layer specification from modelops.yaml."""
    name: str = Field(..., description="Layer name (must be unique)")
    type: str = Field(..., description="Layer type (code, data, config, etc.)")
    files: List[str] = Field(..., description="File patterns to include")
    ignore: List[str] = Field(default_factory=list, description="File patterns to ignore")


class BundleSpec(BaseModel):
    """
    Bundle specification parsed from modelops.yaml.
    
    This represents the developer's intent for bundle structure,
    before any storage decisions are made.
    """
    api_version: str = Field(..., alias="apiVersion", description="API version")
    kind: Literal["Bundle"] = Field(..., description="Resource kind")
    
    # Metadata
    name: str = Field(..., description="Bundle name")
    version: str = Field(..., description="Bundle version")
    description: Optional[str] = Field(default=None, description="Bundle description")
    
    # Specification
    layers: List[LayerSpec] = Field(..., description="Layer definitions")
    roles: Dict[str, List[str]] = Field(..., description="Role to layer mappings")
    external_rules: List[ExternalRule] = Field(default_factory=list, description="External storage rules")
    
    # Storage configuration
    oras_size_limit: int = Field(default=100_000_000, description="Max file size for ORAS storage (bytes)")
    
    @field_validator("roles")
    @classmethod
    def validate_roles(cls, v, info):
        """Validate that roles reference existing layers."""
        # Get layers from the validation context
        if not info.data or "layers" not in info.data:
            return v
        
        layer_names = {layer.name for layer in info.data["layers"]}
        for role_name, role_layers in v.items():
            for layer_name in role_layers:
                if layer_name not in layer_names:
                    raise ValueError(f"Role '{role_name}' references unknown layer '{layer_name}'")
        return v
    
    @classmethod
    def from_yaml_file(cls, path: Path) -> BundleSpec:
        """Load BundleSpec from modelops.yaml file."""
        import yaml
        
        if not path.exists():
            raise FileNotFoundError(f"Bundle specification not found: {path}")
        
        with open(path, 'r') as f:
            data = yaml.safe_load(f)
        
        # Handle nested spec structure
        if "spec" in data:
            spec_data = data["spec"]
            metadata = data.get("metadata", {})
            
            # Merge metadata into spec
            spec_data.update({
                "apiVersion": data.get("apiVersion"),
                "kind": data.get("kind"),
                "name": metadata.get("name"),
                "version": metadata.get("version"), 
                "description": metadata.get("description")
            })
            
            return cls.model_validate(spec_data)
        else:
            return cls.model_validate(data)


class OrasDescriptor(BaseModel):
    """ORAS blob descriptor."""
    digest: str = Field(..., description="Content digest (sha256:...)")
    size: int = Field(..., description="Blob size in bytes")


class ExternalDescriptor(BaseModel):
    """External storage descriptor."""
    uri: str = Field(..., description="External storage URI")
    sha256: str = Field(..., description="Content SHA256 (without sha256: prefix)")
    size: int = Field(..., description="File size in bytes") 
    tier: StorageTier = Field(default=StorageTier.HOT, description="Storage tier")


class LayerIndexEntry(BaseModel):
    """Single file entry in a layer index."""
    path: str = Field(..., description="Relative path in the bundle")
    layer: str = Field(..., description="Layer name this entry belongs to")
    
    # Exactly one of these must be present
    oras: Optional[OrasDescriptor] = Field(default=None, description="ORAS blob reference")
    external: Optional[ExternalDescriptor] = Field(default=None, description="External storage reference")
    
    @field_validator("external")
    @classmethod
    def validate_exactly_one_storage(cls, v, info):
        """Ensure exactly one of oras or external is specified."""
        oras = info.data.get("oras") if info.data else None
        
        if v is None and oras is None:
            raise ValueError("Entry must specify either 'oras' or 'external' storage")
        
        if v is not None and oras is not None:
            raise ValueError("Entry cannot specify both 'oras' and 'external' storage")
        
        return v


class LayerIndex(BaseModel):
    """Layer index document - manifest of files in a layer."""
    media_type: str = Field(
        default="application/json",
        alias="mediaType",
        description="Media type identifier (now uses standard JSON)"
    )
    layer: str = Field(..., description="Layer name")
    entries: List[LayerIndexEntry] = Field(..., description="File entries in this layer")
    
    @computed_field
    @property
    def digest(self) -> str:
        """Compute deterministic digest of this layer index."""
        # Create canonical JSON representation - exclude computed fields!
        data = self.model_dump(by_alias=True, exclude_none=True, exclude={"digest"})
        
        # Sort entries by path for determinism
        data["entries"] = sorted(data["entries"], key=lambda e: e["path"])
        
        # Create canonical JSON (sorted keys, no whitespace)
        canonical = json.dumps(
            data,
            sort_keys=True,
            separators=(',', ':'),
            ensure_ascii=True
        )
        
        # Compute SHA256
        hash_bytes = hashlib.sha256(canonical.encode('utf-8')).hexdigest()
        return f"sha256:{hash_bytes}"


class BundleManifest(BaseModel):
    """Top-level bundle manifest document."""
    media_type: str = Field(
        default="application/json",
        alias="mediaType",
        description="Media type identifier (now uses standard JSON)"
    )
    
    # Bundle identity
    name: str = Field(..., description="Bundle name")
    version: str = Field(..., description="Bundle version")
    description: Optional[str] = Field(default=None, description="Bundle description")
    
    # Structure
    roles: Dict[str, List[str]] = Field(..., description="Role to layer mappings")
    layers: Dict[str, str] = Field(..., description="Layer name to layer index digest mapping")
    external_index_present: bool = Field(default=True, description="Whether external references exist")
    
    @computed_field
    @property
    def digest(self) -> str:
        """Compute deterministic digest of this bundle manifest."""
        # Create canonical JSON representation - exclude computed fields!
        data = self.model_dump(by_alias=True, exclude_none=True, exclude={"digest"})
        
        # Create canonical JSON (sorted keys, no whitespace)
        canonical = json.dumps(
            data,
            sort_keys=True,
            separators=(',', ':'),
            ensure_ascii=True
        )
        
        # Compute SHA256
        hash_bytes = hashlib.sha256(canonical.encode('utf-8')).hexdigest()
        return f"sha256:{hash_bytes}"


class FileEntry(BaseModel):
    """File discovered during scanning."""
    src_path: Path = Field(..., description="Source file path")
    artifact_path: str = Field(..., description="Path in the artifact")
    size: int = Field(..., description="File size in bytes")
    sha256: str = Field(..., description="File SHA256 hash")
    layer: str = Field(..., description="Layer this file belongs to")


class StorageDecision(str, Enum):
    """Storage backend decision for a file."""
    ORAS = "oras"
    EXTERNAL = "external"


class LayerPlan(BaseModel):
    """Storage plan for a single layer."""
    name: str = Field(..., description="Layer name")
    files: List[FileEntry] = Field(..., description="Files in this layer")
    storage_decisions: Dict[str, StorageDecision] = Field(..., description="Path to storage decision")
    
    @computed_field
    @property
    def oras_files(self) -> List[FileEntry]:
        """Files that will be stored in ORAS."""
        return [f for f in self.files if self.storage_decisions[f.artifact_path] == StorageDecision.ORAS]
    
    @computed_field 
    @property
    def external_files(self) -> List[FileEntry]:
        """Files that will be stored externally."""
        return [f for f in self.files if self.storage_decisions[f.artifact_path] == StorageDecision.EXTERNAL]


class StoragePlan(BaseModel):
    """Complete storage plan for bundle creation."""
    spec: BundleSpec = Field(..., description="Bundle specification")
    layer_plans: Dict[str, LayerPlan] = Field(..., description="Per-layer storage plans")
    working_dir: Path = Field(..., description="Source working directory")
    
    @computed_field
    @property
    def all_oras_files(self) -> List[FileEntry]:
        """All files that will be stored in ORAS across all layers."""
        files = []
        for layer_plan in self.layer_plans.values():
            files.extend(layer_plan.oras_files)
        return files
    
    @computed_field
    @property
    def all_external_files(self) -> List[FileEntry]:
        """All files that will be stored externally across all layers.""" 
        files = []
        for layer_plan in self.layer_plans.values():
            files.extend(layer_plan.external_files)
        return files


# Media type constants for easy import
BUNDLE_MANIFEST_TYPE = "application/json"
LAYER_INDEX_TYPE = "application/json"