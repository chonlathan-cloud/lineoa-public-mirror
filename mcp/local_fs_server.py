import os
import json
import logging
from mcp.server.fastmcp import FastMCP

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("mcp-local-fs")

# Initialize FastMCP
mcp = FastMCP("local-fs")

# Root directory (current working directory)
ROOT_DIR = os.path.abspath(".")

# Allowed paths (simplified implementation of mcp.yaml rules)
ALLOWED_EXTENSIONS = {".py", ".html", ".css", ".js", ".md", ".txt", ".json", ".yaml", ".yml", ".sh", "Dockerfile"}
DENY_DIRS = {".git", "__pycache__", "venv", ".venv", ".env"}

def is_allowed(path: str) -> bool:
    """Check if path is allowed based on simple rules."""
    abs_path = os.path.abspath(path)
    if not abs_path.startswith(ROOT_DIR):
        return False
    
    rel_path = os.path.relpath(abs_path, ROOT_DIR)
    parts = rel_path.split(os.sep)
    
    # Check deny dirs
    for part in parts:
        if part in DENY_DIRS:
            return False
            
    # Check extension for files
    if os.path.isfile(abs_path):
        _, ext = os.path.splitext(abs_path)
        if ext not in ALLOWED_EXTENSIONS:
            return False
            
    return True

@mcp.resource("file://{path}")
def read_file(path: str) -> str:
    """Read a local file if allowed."""
    # Handle URL encoding or simple path
    clean_path = path.lstrip("/")
    full_path = os.path.join(ROOT_DIR, clean_path)
    
    if not os.path.exists(full_path):
        return f"Error: File not found: {clean_path}"
        
    if not is_allowed(full_path):
        return f"Error: Access denied to {clean_path}"
        
    try:
        with open(full_path, "r", encoding="utf-8") as f:
            return f.read()
    except Exception as e:
        return f"Error reading file: {str(e)}"

@mcp.resource("dir://{path}")
def list_directory(path: str) -> str:
    """List directory contents."""
    clean_path = path.lstrip("/")
    if clean_path == "" or clean_path == ".":
        full_path = ROOT_DIR
    else:
        full_path = os.path.join(ROOT_DIR, clean_path)
        
    if not os.path.exists(full_path):
        return f"Error: Directory not found: {clean_path}"
        
    if not is_allowed(full_path):
        return f"Error: Access denied to {clean_path}"
        
    try:
        items = []
        for item in os.listdir(full_path):
            item_path = os.path.join(full_path, item)
            if is_allowed(item_path):
                items.append({
                    "name": item,
                    "type": "dir" if os.path.isdir(item_path) else "file"
                })
        return json.dumps(items, indent=2)
    except Exception as e:
        return f"Error listing directory: {str(e)}"

if __name__ == "__main__":
    mcp.run()
