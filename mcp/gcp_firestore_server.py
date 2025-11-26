import os
import json
import logging
from typing import List, Optional
from mcp.server.fastmcp import FastMCP
from google.cloud import firestore

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("mcp-firestore")

# Initialize FastMCP
mcp = FastMCP("gcp-firestore")

# Initialize Firestore Client
# Assumes ADC or GOOGLE_APPLICATION_CREDENTIALS is set
try:
    db = firestore.Client()
    logger.info(f"Connected to Firestore project: {db.project}")
except Exception as e:
    logger.error(f"Failed to connect to Firestore: {e}")
    db = None

@mcp.resource("firestore://{collection}/{document_id}")
def get_firestore_document(collection: str, document_id: str) -> str:
    """Read a specific document from Firestore."""
    if not db:
        return "Error: Firestore client not initialized."
    
    try:
        doc_ref = db.collection(collection).document(document_id)
        doc = doc_ref.get()
        if doc.exists:
            return json.dumps(doc.to_dict(), default=str, indent=2)
        else:
            return f"Error: Document {collection}/{document_id} not found."
    except Exception as e:
        return f"Error reading document: {str(e)}"

@mcp.resource("firestore://{collection}")
def list_firestore_collection(collection: str) -> str:
    """List documents in a collection (limit 20)."""
    if not db:
        return "Error: Firestore client not initialized."
    
    try:
        docs = db.collection(collection).limit(20).stream()
        results = []
        for doc in docs:
            data = doc.to_dict()
            data["_id"] = doc.id
            results.append(data)
        return json.dumps(results, default=str, indent=2)
    except Exception as e:
        return f"Error listing collection: {str(e)}"

if __name__ == "__main__":
    mcp.run()
