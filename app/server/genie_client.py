"""Async client for Databricks Genie Room API."""

import aiohttp
import asyncio
from typing import Optional, Dict, Any, List
from dataclasses import dataclass


@dataclass
class GenieMessage:
    """Represents a message in a Genie conversation."""
    id: str
    content: str
    status: str
    attachments: List[Dict[str, Any]]
    query: Optional[str] = None
    error: Optional[str] = None


@dataclass
class GenieResponse:
    """Response from Genie Room API."""
    conversation_id: str
    message_id: str
    status: str
    content: Optional[str] = None
    query: Optional[str] = None
    results: Optional[List[Dict[str, Any]]] = None
    error: Optional[str] = None


class GenieClient:
    """Async client for interacting with Genie Room API."""

    def __init__(self, space_id: str, token: str, host: str):
        self.space_id = space_id
        self.token = token
        self.host = host.rstrip("/")
        self._session: Optional[aiohttp.ClientSession] = None

    @property
    def headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json"
        }

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()

    async def start_conversation(self, question: str) -> GenieResponse:
        """Start a new Genie conversation with a question."""
        url = f"{self.host}/api/2.0/genie/spaces/{self.space_id}/start-conversation"
        payload = {"content": question}

        session = await self._get_session()
        async with session.post(url, json=payload, headers=self.headers) as resp:
            if resp.status != 200:
                error = await resp.text()
                return GenieResponse(
                    conversation_id="",
                    message_id="",
                    status="FAILED",
                    error=f"API error {resp.status}: {error}"
                )

            data = await resp.json()
            conv_id = data.get("conversation_id", "")
            msg_id = data.get("message_id", "")

            return GenieResponse(
                conversation_id=conv_id,
                message_id=msg_id,
                status="PENDING"
            )

    async def get_message(self, conversation_id: str, message_id: str) -> GenieResponse:
        """Get the status and content of a message."""
        url = f"{self.host}/api/2.0/genie/spaces/{self.space_id}/conversations/{conversation_id}/messages/{message_id}"

        session = await self._get_session()
        async with session.get(url, headers=self.headers) as resp:
            if resp.status != 200:
                error = await resp.text()
                return GenieResponse(
                    conversation_id=conversation_id,
                    message_id=message_id,
                    status="FAILED",
                    error=f"API error {resp.status}: {error}"
                )

            data = await resp.json()
            status = data.get("status", "PENDING")

            # Extract content and query from attachments
            content = None
            query = None
            attachments = data.get("attachments", [])

            for att in attachments:
                if att.get("type") == "TEXT":
                    content = att.get("text", {}).get("content", "")
                elif att.get("type") == "QUERY":
                    query_data = att.get("query", {})
                    query = query_data.get("query", "")

            return GenieResponse(
                conversation_id=conversation_id,
                message_id=message_id,
                status=status,
                content=content,
                query=query
            )

    async def get_query_results(
        self,
        conversation_id: str,
        message_id: str,
        attachment_id: str
    ) -> List[Dict[str, Any]]:
        """Fetch query results from an attachment."""
        url = (
            f"{self.host}/api/2.0/genie/spaces/{self.space_id}"
            f"/conversations/{conversation_id}/messages/{message_id}"
            f"/query-result/{attachment_id}"
        )

        session = await self._get_session()
        async with session.get(url, headers=self.headers) as resp:
            if resp.status != 200:
                return []

            data = await resp.json()

            # Parse statement response format
            columns = []
            rows = []

            manifest = data.get("statement_response", {}).get("manifest", {})
            schema = manifest.get("schema", {}).get("columns", [])
            columns = [col.get("name", f"col_{i}") for i, col in enumerate(schema)]

            result = data.get("statement_response", {}).get("result", {})
            data_array = result.get("data_array", [])

            # Convert to list of dicts
            results = []
            for row in data_array:
                results.append(dict(zip(columns, row)))

            return results

    async def follow_up(self, conversation_id: str, question: str) -> GenieResponse:
        """Ask a follow-up question in an existing conversation."""
        url = f"{self.host}/api/2.0/genie/spaces/{self.space_id}/conversations/{conversation_id}/messages"
        payload = {"content": question}

        session = await self._get_session()
        async with session.post(url, json=payload, headers=self.headers) as resp:
            if resp.status != 200:
                error = await resp.text()
                return GenieResponse(
                    conversation_id=conversation_id,
                    message_id="",
                    status="FAILED",
                    error=f"API error {resp.status}: {error}"
                )

            data = await resp.json()
            msg_id = data.get("id", "")

            return GenieResponse(
                conversation_id=conversation_id,
                message_id=msg_id,
                status="PENDING"
            )

    async def poll_until_complete(
        self,
        conversation_id: str,
        message_id: str,
        timeout: float = 120.0,
        poll_interval: float = 2.0
    ) -> GenieResponse:
        """Poll for message completion with timeout."""
        start_time = asyncio.get_event_loop().time()

        while True:
            elapsed = asyncio.get_event_loop().time() - start_time
            if elapsed > timeout:
                return GenieResponse(
                    conversation_id=conversation_id,
                    message_id=message_id,
                    status="TIMEOUT",
                    error="Query timed out"
                )

            response = await self.get_message(conversation_id, message_id)

            if response.status in ("COMPLETED", "FAILED", "CANCELLED"):
                return response

            await asyncio.sleep(poll_interval)
