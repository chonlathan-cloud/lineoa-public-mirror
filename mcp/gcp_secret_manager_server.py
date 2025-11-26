import os
import json
import logging
from mcp.server.fastmcp import FastMCP
from google.cloud import secretmanager

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("mcp-secrets")

# Initialize FastMCP
mcp = FastMCP("gcp-secrets")

# Initialize Secret Manager Client
try:
    client = secretmanager.SecretManagerServiceClient()
    # Attempt to get project ID from env or default
    project_id = os.environ.get("PROJECT_ID", "lineoa-g49") 
    parent = f"projects/{project_id}"
    logger.info(f"Connected to Secret Manager for project: {project_id}")
except Exception as e:
    logger.error(f"Failed to connect to Secret Manager: {e}")
    client = None
    parent = None

@mcp.resource("secrets://list")
def list_secrets() -> str:
    """List available secrets (metadata only)."""
    if not client or not parent:
        return "Error: Secret Manager client not initialized."
    
    try:
        results = []
        for secret in client.list_secrets(request={"parent": parent}):
            results.append({
                "name": secret.name,
                "create_time": str(secret.create_time),
                "labels": dict(secret.labels)
            })
        return json.dumps(results, indent=2)
    except Exception as e:
        return f"Error listing secrets: {str(e)}"

@mcp.resource("secrets://{secret_id}/metadata")
def get_secret_metadata(secret_id: str) -> str:
    """Get metadata for a specific secret. DOES NOT RETURN VALUE."""
    if not client or not parent:
        return "Error: Secret Manager client not initialized."
    
    try:
        name = f"{parent}/secrets/{secret_id}"
        secret = client.get_secret(request={"name": name})
        return json.dumps({
            "name": secret.name,
            "create_time": str(secret.create_time),
            "replication": str(secret.replication),
            "labels": dict(secret.labels)
        }, indent=2)
    except Exception as e:
        return f"Error getting secret metadata: {str(e)}"

if __name__ == "__main__":
    mcp.run()
