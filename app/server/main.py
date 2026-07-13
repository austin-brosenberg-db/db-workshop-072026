"""FastAPI backend for Illumia Genie Portal."""

import os
from typing import Optional
from fastapi import FastAPI, Request, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel

from .config import get_workspace_host, get_auth_token, GENIE_SPACE_ID
from .genie_client import GenieClient

app = FastAPI(title="Illumia Genie Portal")


class ChatRequest(BaseModel):
    """Request to start a chat or follow up."""
    question: str
    conversation_id: Optional[str] = None


class ChatResponse(BaseModel):
    """Response from chat endpoint."""
    conversation_id: str
    message_id: str
    status: str
    content: Optional[str] = None
    query: Optional[str] = None
    results: Optional[list] = None
    error: Optional[str] = None


def get_user_token(request: Request) -> Optional[str]:
    """Extract user token from forwarded headers."""
    return request.headers.get("x-forwarded-access-token")


@app.post("/api/chat", response_model=ChatResponse)
async def chat(request: Request, body: ChatRequest):
    """
    Start a new conversation or follow up in an existing one.
    Uses the user's identity if available, otherwise falls back to service principal.
    """
    user_token = get_user_token(request)
    token = get_auth_token(user_token)
    host = get_workspace_host()

    client = GenieClient(GENIE_SPACE_ID, token, host)

    try:
        if body.conversation_id:
            # Follow-up in existing conversation
            response = await client.follow_up(body.conversation_id, body.question)
        else:
            # Start new conversation
            response = await client.start_conversation(body.question)

        if response.status == "FAILED":
            raise HTTPException(status_code=500, detail=response.error)

        return ChatResponse(
            conversation_id=response.conversation_id,
            message_id=response.message_id,
            status=response.status,
            error=response.error
        )
    finally:
        await client.close()


@app.get("/api/chat/{conversation_id}/{message_id}/status", response_model=ChatResponse)
async def get_status(request: Request, conversation_id: str, message_id: str):
    """Poll for the status of a pending message."""
    user_token = get_user_token(request)
    token = get_auth_token(user_token)
    host = get_workspace_host()

    client = GenieClient(GENIE_SPACE_ID, token, host)

    try:
        response = await client.get_message(conversation_id, message_id)

        return ChatResponse(
            conversation_id=response.conversation_id,
            message_id=response.message_id,
            status=response.status,
            content=response.content,
            query=response.query,
            error=response.error
        )
    finally:
        await client.close()


@app.get("/api/chat/{conversation_id}/{message_id}/results/{attachment_id}")
async def get_results(
    request: Request,
    conversation_id: str,
    message_id: str,
    attachment_id: str
):
    """Fetch query results from an attachment."""
    user_token = get_user_token(request)
    token = get_auth_token(user_token)
    host = get_workspace_host()

    client = GenieClient(GENIE_SPACE_ID, token, host)

    try:
        results = await client.get_query_results(conversation_id, message_id, attachment_id)
        return {"results": results}
    finally:
        await client.close()


@app.get("/api/health")
async def health():
    """Health check endpoint."""
    return {"status": "healthy", "genie_space_id": GENIE_SPACE_ID}


# Serve static files (React build) in production
static_dir = os.path.join(os.path.dirname(__file__), "..", "static")
if os.path.exists(static_dir):
    app.mount("/assets", StaticFiles(directory=os.path.join(static_dir, "assets")), name="assets")

    @app.get("/{full_path:path}")
    async def serve_spa(full_path: str):
        """Serve the React SPA for all non-API routes."""
        index_path = os.path.join(static_dir, "index.html")
        if os.path.exists(index_path):
            return FileResponse(index_path)
        raise HTTPException(status_code=404, detail="Not found")
