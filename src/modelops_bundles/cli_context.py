"""
CLI Context for managing application dependencies.

Provides a clean way to manage CLI-level dependencies like settings and registry
instances, avoiding global state and enabling proper dependency injection.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from .settings import Settings, create_settings_from_env
from .storage.oras_bundle_registry import OrasBundleRegistry


@dataclass
class CLIContext:
    """
    Shared context for CLI commands.
    
    Manages application-level dependencies (settings, registry) that are
    initialized once and shared across a CLI command execution.
    
    This eliminates the need for global state and provides clean dependency
    injection for CLI commands.
    """
    settings: Settings
    _registry: Optional[OrasBundleRegistry] = None
    
    @classmethod
    def from_env(cls) -> CLIContext:
        """
        Create CLI context from environment variables.
        
        Returns:
            CLIContext with settings loaded from environment
        """
        settings = create_settings_from_env()
        return cls(settings=settings)
    
    @property
    def registry(self) -> OrasBundleRegistry:
        """
        Get or create registry instance (lazy initialization).
        
        The registry is created on first access and reused for subsequent calls.
        This avoids creating multiple registry instances within a single CLI command.
        
        Returns:
            OrasBundleRegistry instance
        """
        if self._registry is None:
            self._registry = OrasBundleRegistry(self.settings)
        return self._registry