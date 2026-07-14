"""Configuration and authentication helpers for Databricks App."""

import os
from typing import Optional

# Check for Databricks environment - DATABRICKS_HOST is set in Databricks Apps
DATABRICKS_HOST_ENV = os.environ.get("DATABRICKS_HOST")
DATABRICKS_APP_NAME = os.environ.get("DATABRICKS_APP_NAME")

# We're in Databricks if DATABRICKS_HOST is set (more reliable than checking APP_NAME)
IS_DATABRICKS_ENV = bool(DATABRICKS_HOST_ENV)

# Required environment variables
GENIE_SPACE_ID = os.environ.get("GENIE_SPACE_ID")
DASHBOARD_ID = os.environ.get("DASHBOARD_ID")

# For local development only - no hardcoded default
LOCAL_DEV_PROFILE = os.environ.get("DATABRICKS_PROFILE")


def _get_workspace_client():
    """Get a WorkspaceClient for the current environment."""
    from databricks.sdk import WorkspaceClient

    if IS_DATABRICKS_ENV:
        # In Databricks - SDK auto-detects credentials
        try:
            return WorkspaceClient()
        except Exception as e:
            raise RuntimeError(
                f"Failed to create WorkspaceClient in Databricks environment. "
                f"DATABRICKS_HOST={DATABRICKS_HOST_ENV}, error: {e}"
            ) from e
    elif LOCAL_DEV_PROFILE:
        # Local dev with explicit profile
        return WorkspaceClient(profile=LOCAL_DEV_PROFILE)
    else:
        raise RuntimeError(
            "No Databricks authentication configured. "
            "Set DATABRICKS_HOST (for Databricks Apps) or DATABRICKS_PROFILE (for local dev)."
        )


def get_workspace_id() -> str:
    """Get the workspace ID for embed URLs."""
    client = _get_workspace_client()
    return str(client.get_workspace_id())


def get_workspace_host() -> str:
    """Get workspace host with https:// prefix."""
    if IS_DATABRICKS_ENV and DATABRICKS_HOST_ENV:
        host = DATABRICKS_HOST_ENV
        if not host.startswith("http"):
            host = f"https://{host}"
        return host

    # Fall back to SDK config
    client = _get_workspace_client()
    return client.config.host


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


def get_auth_token(forwarded_token: Optional[str] = None) -> str:
    """
    Get authentication token for API calls.

    Priority:
    1. User's forwarded token (identity passthrough)
    2. SDK-managed token (auto-detects Databricks App or local config)
    """
    if forwarded_token:
        return forwarded_token

    client = _get_workspace_client()
    auth_headers = client.config.authenticate()
    if auth_headers and "Authorization" in auth_headers:
        return auth_headers["Authorization"].replace("Bearer ", "")

    raise RuntimeError("No authentication token available")
