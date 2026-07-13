"""Configuration and authentication helpers for Databricks App."""

import os
from typing import Optional

# Detect if running in Databricks App context
IS_DATABRICKS_APP = bool(os.environ.get("DATABRICKS_APP_NAME"))

# Genie Space ID from app resource
GENIE_SPACE_ID = os.environ.get("GENIE_SPACE_ID", "01f17eb9d71413c99a1aa2e5716ddf23")


def get_workspace_host() -> str:
    """Get workspace host with https:// prefix."""
    if IS_DATABRICKS_APP:
        host = os.environ.get("DATABRICKS_HOST", "")
        if host and not host.startswith("http"):
            host = f"https://{host}"
        return host

    # Local development - use CLI profile
    from databricks.sdk import WorkspaceClient
    profile = os.environ.get("DATABRICKS_PROFILE", "illumia-demo")
    client = WorkspaceClient(profile=profile)
    return client.config.host


def get_auth_token(forwarded_token: Optional[str] = None) -> str:
    """
    Get authentication token for API calls.

    Priority:
    1. User's forwarded token (identity passthrough)
    2. Service principal token (Databricks App)
    3. CLI profile token (local development)
    """
    if forwarded_token:
        return forwarded_token

    if IS_DATABRICKS_APP:
        from databricks.sdk import WorkspaceClient
        client = WorkspaceClient()
        auth_headers = client.config.authenticate()
        if auth_headers and "Authorization" in auth_headers:
            return auth_headers["Authorization"].replace("Bearer ", "")

    # Local development fallback
    from databricks.sdk import WorkspaceClient
    profile = os.environ.get("DATABRICKS_PROFILE", "illumia-demo")
    client = WorkspaceClient(profile=profile)
    auth_headers = client.config.authenticate()
    if auth_headers and "Authorization" in auth_headers:
        return auth_headers["Authorization"].replace("Bearer ", "")

    raise RuntimeError("No authentication token available")
