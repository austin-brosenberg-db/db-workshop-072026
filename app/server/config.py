"""Configuration and authentication helpers for Databricks App."""

import os
from typing import Optional

# Detect if running in Databricks App context
IS_DATABRICKS_APP = bool(os.environ.get("DATABRICKS_APP_NAME"))

# Required environment variables
GENIE_SPACE_ID = os.environ.get("GENIE_SPACE_ID")
DASHBOARD_ID = os.environ.get("DASHBOARD_ID")


def get_workspace_id() -> str:
    """Get the workspace ID for embed URLs."""
    from databricks.sdk import WorkspaceClient

    if IS_DATABRICKS_APP:
        client = WorkspaceClient()
    else:
        profile = os.environ.get("DATABRICKS_PROFILE", "illumia-demo")
        client = WorkspaceClient(profile=profile)

    return str(client.get_workspace_id())


def get_dashboard_embed_url() -> Optional[str]:
    """Construct the full dashboard embed URL."""
    if not DASHBOARD_ID:
        return None

    host = get_workspace_host()
    workspace_id = get_workspace_id()

    # Remove trailing slash and ensure https://
    host = host.rstrip("/")
    if not host.startswith("http"):
        host = f"https://{host}"

    return f"{host}/embed/dashboardsv3/{DASHBOARD_ID}?o={workspace_id}"


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
