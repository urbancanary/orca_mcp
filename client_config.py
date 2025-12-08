"""
Client Configuration Manager for Orca MCP

Handles client routing, configuration loading, and service registry access.
"""

import os
import json
from pathlib import Path
from typing import Dict, Any, Optional

# Load service registry
SERVICE_REGISTRY_PATH = Path(__file__).parent / "service_registry.json"

class ClientConfig:
    """Manages client-specific configuration and routing"""

    def __init__(self, client_id: Optional[str] = None):
        """
        Initialize client configuration

        Args:
            client_id: Client identifier. If None, reads from CLIENT_ID env var
        """
        self.client_id = client_id or os.getenv("CLIENT_ID", "guinness")
        self.registry = self._load_service_registry()
        self.client_config = self._load_client_config()

    def _load_service_registry(self) -> Dict[str, Any]:
        """Load the service registry"""
        with open(SERVICE_REGISTRY_PATH, 'r') as f:
            return json.load(f)

    def _load_client_config(self) -> Dict[str, Any]:
        """Load configuration for the current client"""
        clients = self.registry.get("clients", {})

        if self.client_id not in clients:
            raise ValueError(f"Client '{self.client_id}' not found in service registry")

        return clients[self.client_id]

    def get_service(self, service_name: str) -> Dict[str, Any]:
        """
        Get service configuration by name

        Args:
            service_name: Name of the service (e.g., 'auth_mcp', 'bigquery')

        Returns:
            Service configuration dictionary
        """
        services = self.registry.get("services", {})

        if service_name not in services:
            raise ValueError(f"Service '{service_name}' not found in registry")

        return services[service_name]

    def get_database_registry_path(self) -> Path:
        """Get path to client's database registry"""
        registry_path = self.client_config.get("database_registry")

        # Support template variables like {client_id}
        registry_path = registry_path.replace("{client_id}", self.client_id)

        # Resolve relative to project root
        project_root = Path(__file__).parent.parent
        return project_root / registry_path

    def get_bigquery_dataset(self) -> str:
        """Get BigQuery dataset name for this client"""
        dataset = self.client_config.get("bigquery_dataset", "portfolio_data")
        return dataset.replace("{client_id}", self.client_id)

    def get_auth_token(self, service_name: str = "auth_mcp") -> str:
        """
        Get authentication token for a service

        Args:
            service_name: Service to get token for

        Returns:
            Authentication token
        """
        service = self.get_service(service_name)
        auth_config = service.get("auth", {})

        # Try environment variable first
        token_env = auth_config.get("token_env_var")
        if token_env and os.getenv(token_env):
            return os.getenv(token_env)

        # Try MCP_AUTH_TOKEN as fallback (Railway uses this name)
        if os.getenv("MCP_AUTH_TOKEN"):
            return os.getenv("MCP_AUTH_TOKEN")

        # Fall back to default
        return auth_config.get("default_token", "")

    def is_feature_locked(self, feature_name: str) -> bool:
        """
        Check if a feature is locked (xtrillion IP)

        Args:
            feature_name: Name of the feature/service

        Returns:
            True if locked, False if client can modify
        """
        service = self.get_service(feature_name)
        return service.get("locked", False)

    def get_custom_features_path(self) -> Path:
        """Get path to client's custom features directory"""
        custom_path = self.client_config.get("custom_features_path", f"clients/{self.client_id}")
        custom_path = custom_path.replace("{client_id}", self.client_id)

        project_root = Path(__file__).parent.parent
        return project_root / custom_path

    def get_license_tier(self) -> str:
        """Get client's license tier"""
        return self.client_config.get("license_tier", "basic")

    def get_tier_config(self) -> Dict[str, Any]:
        """Get full license tier configuration"""
        tier = self.get_license_tier()
        tiers = self.registry.get("license_tiers", {})
        return tiers.get(tier, tiers.get("basic", {}))

    def is_tool_allowed(self, tool_name: str) -> bool:
        """
        Check if client's license allows this tool

        Args:
            tool_name: Name of the MCP tool

        Returns:
            True if allowed, False if requires upgrade
        """
        if not self.client_config.get("active", False):
            return False

        tier_config = self.get_tier_config()
        allowed_tools = tier_config.get("allowed_tools", [])

        # Wildcard means all tools allowed
        if "*" in allowed_tools:
            return True

        return tool_name in allowed_tools

    def has_feature(self, feature_name: str) -> bool:
        """
        Check if client's license includes a feature

        Args:
            feature_name: Feature name (e.g., 'rvm_analytics')

        Returns:
            True if feature is enabled
        """
        tier_config = self.get_tier_config()
        features = tier_config.get("features", {})
        return features.get(feature_name, False)

    def get_limit(self, limit_name: str) -> Optional[int]:
        """
        Get client's usage limit

        Args:
            limit_name: Limit name (e.g., 'max_queries_per_day')

        Returns:
            Limit value or None if unlimited
        """
        tier_config = self.get_tier_config()
        limits = tier_config.get("limits", {})
        return limits.get(limit_name)

    def __repr__(self):
        return f"ClientConfig(client_id='{self.client_id}', tier='{self.get_license_tier()}', dataset='{self.get_bigquery_dataset()}')"


# Singleton instance for easy access
_default_config = None

def get_client_config(client_id: Optional[str] = None) -> ClientConfig:
    """
    Get or create the default client configuration

    Args:
        client_id: Optional client ID. If None, uses environment variable

    Returns:
        ClientConfig instance
    """
    global _default_config

    if _default_config is None or (client_id and client_id != _default_config.client_id):
        _default_config = ClientConfig(client_id)

    return _default_config
