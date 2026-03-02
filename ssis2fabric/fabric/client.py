"""
Fabric REST API client using interactive user authentication.

Authentication flow
-------------------
Uses azure-identity InteractiveBrowserCredential (or DeviceCodeCredential as
fallback) to obtain a token for the Fabric API on behalf of the signed-in user.

The token scope used is: https://api.fabric.microsoft.com/.default

Fabric REST API base URLs
--------------------------
  Items / pipelines / dataflows:
    https://api.fabric.microsoft.com/v1

  Connections (Power Platform / Fabric core):
    https://api.fabric.microsoft.com/v1

Key endpoints used
------------------
  Create item:         POST /workspaces/{wid}/items
  Update definition:   POST /workspaces/{wid}/items/{iid}/updateDefinition
  Create connection:   POST /v1/connections
  List items:          GET  /workspaces/{wid}/items
"""
import json
import time
from typing import Any, Dict, List, Optional

import requests

try:
    from azure.identity import InteractiveBrowserCredential
    _HAS_AZURE_IDENTITY = True
except ImportError:
    _HAS_AZURE_IDENTITY = False

FABRIC_API_BASE = "https://api.fabric.microsoft.com/v1"
FABRIC_SCOPE = "https://api.fabric.microsoft.com/.default"

MAX_POLL_SECONDS = 300   # 5-minute timeout for LRO polling
POLL_INTERVAL = 5        # seconds between polls


class FabricAuthError(RuntimeError):
    pass


class FabricAPIError(RuntimeError):
    def __init__(self, status_code: int, message: str, body: str = ""):
        super().__init__(f"HTTP {status_code}: {message}\n{body}")
        self.status_code = status_code
        self.response_body = body


class FabricClient:
    """
    Thin wrapper around the Fabric REST API, authenticating as the current user.
    """

    def __init__(self, verbose: bool = False):
        if not _HAS_AZURE_IDENTITY:
            raise FabricAuthError(
                "azure-identity is required for user authentication. "
                "Run: pip install azure-identity"
            )
        self._verbose = verbose
        self._credential = InteractiveBrowserCredential()
        self._token_cache: Optional[str] = None

    # ------------------------------------------------------------------
    # Auth helpers
    # ------------------------------------------------------------------

    def _get_token(self) -> str:
        """Obtain (or refresh) a bearer token."""
        token = self._credential.get_token(FABRIC_SCOPE)
        return token.token

    def _headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self._get_token()}",
            "Content-Type": "application/json",
        }

    # ------------------------------------------------------------------
    # Low-level HTTP helpers
    # ------------------------------------------------------------------

    def _log(self, msg: str) -> None:
        if self._verbose:
            print(f"  [fabric] {msg}")

    def _request(
        self,
        method: str,
        url: str,
        payload: Optional[Dict] = None,
        params: Optional[Dict] = None,
    ) -> requests.Response:
        self._log(f"{method} {url}")
        resp = requests.request(
            method,
            url,
            headers=self._headers(),
            json=payload,
            params=params,
            timeout=60,
        )
        if self._verbose and resp.text:
            self._log(f"  → {resp.status_code}: {resp.text[:400]}")
        return resp

    def _raise_for_status(self, resp: requests.Response) -> None:
        if resp.status_code >= 400:
            try:
                body = resp.json()
                msg = body.get("message") or body.get("error", {}).get("message") or resp.text
            except Exception:
                msg = resp.text
            raise FabricAPIError(resp.status_code, msg, resp.text)

    def _poll_lro(self, location: str) -> Dict[str, Any]:
        """Poll a Long-Running Operation until it completes."""
        deadline = time.time() + MAX_POLL_SECONDS
        while time.time() < deadline:
            time.sleep(POLL_INTERVAL)
            resp = self._request("GET", location)
            if resp.status_code == 200:
                return resp.json()
            if resp.status_code in (202, 204):
                continue
            self._raise_for_status(resp)
        raise TimeoutError(f"LRO timed out after {MAX_POLL_SECONDS}s: {location}")

    # ------------------------------------------------------------------
    # Item creation
    # ------------------------------------------------------------------

    def list_items(self, workspace_id: str, item_type: str) -> List[Dict[str, Any]]:
        """Return all items of *item_type* in the workspace."""
        url = f"{FABRIC_API_BASE}/workspaces/{workspace_id}/items"
        resp = self._request("GET", url, params={"type": item_type})
        if resp.status_code == 200:
            return resp.json().get("value", [])
        return []

    def find_item_by_name(
        self, workspace_id: str, display_name: str, item_type: str
    ) -> Optional[Dict[str, Any]]:
        """Return the first item matching *display_name* and *item_type*, or None."""
        for item in self.list_items(workspace_id, item_type):
            if item.get("displayName") == display_name:
                return item
        return None

    def create_item(
        self,
        workspace_id: str,
        display_name: str,
        item_type: str,
        description: str = "",
        folder_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Create a Fabric item (DataPipeline, Dataflow, etc.) without a definition.
        If an item with the same name already exists, returns the existing item.

        Returns the item JSON (including its 'id').
        """
        url = f"{FABRIC_API_BASE}/workspaces/{workspace_id}/items"
        payload: Dict[str, Any] = {
            "displayName": display_name,
            "type": item_type,
        }
        if description:
            payload["description"] = description
        if folder_id:
            payload["folderId"] = folder_id

        resp = self._request("POST", url, payload=payload)
        if resp.status_code in (200, 201):
            return resp.json()
        if resp.status_code == 202:
            # Long-running operation
            location = resp.headers.get("Location") or resp.headers.get("location")
            if location:
                return self._poll_lro(location)
        # Item name already in use — find existing item and return it
        if resp.status_code == 400:
            try:
                err = resp.json().get("errorCode", "")
            except Exception:
                err = ""
            if "AlreadyInUse" in err or "alreadyinuse" in err.lower():
                existing = self.find_item_by_name(workspace_id, display_name, item_type)
                if existing:
                    print(f"  [info] '{display_name}' already exists — updating definition.")
                    return existing
        self._raise_for_status(resp)
        return resp.json()

    def update_item_definition(
        self,
        workspace_id: str,
        item_id: str,
        definition: Dict[str, Any],
    ) -> None:
        """
        Upload / update a Fabric item's definition.
        Endpoint: POST /workspaces/{wid}/items/{iid}/updateDefinition
        """
        url = f"{FABRIC_API_BASE}/workspaces/{workspace_id}/items/{item_id}/updateDefinition"
        resp = self._request("POST", url, payload=definition)
        if resp.status_code in (200, 201, 204):
            return
        if resp.status_code == 202:
            location = resp.headers.get("Location") or resp.headers.get("location")
            if location:
                self._poll_lro(location)
            return
        self._raise_for_status(resp)

    def create_pipeline(
        self,
        workspace_id: str,
        display_name: str,
        definition: Dict[str, Any],
        description: str = "",
        folder_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Create a DataPipeline item and upload its definition."""
        item = self.create_item(
            workspace_id=workspace_id,
            display_name=display_name,
            item_type="DataPipeline",
            description=description,
            folder_id=folder_id,
        )
        item_id = item.get("id") or item.get("objectId")
        if not item_id:
            raise FabricAPIError(0, f"No item ID returned when creating pipeline '{display_name}'")
        self._log(f"Created pipeline '{display_name}' → id={item_id}")
        self.update_item_definition(workspace_id, item_id, definition)
        item["id"] = item_id
        return item

    def create_dataflow(
        self,
        workspace_id: str,
        display_name: str,
        definition: Dict[str, Any],
        description: str = "",
        folder_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Create a Dataflow Gen2 item using the dedicated /dataflows endpoint,
        embedding the definition in the creation request.
        If the item already exists, update its definition via updateDefinition.
        """
        url = f"{FABRIC_API_BASE}/workspaces/{workspace_id}/dataflows"
        payload: Dict[str, Any] = {"displayName": display_name}
        if description:
            payload["description"] = description
        if folder_id:
            payload["folderId"] = folder_id
        payload.update(definition)   # embeds {"definition": {"parts": [...]}}

        resp = self._request("POST", url, payload=payload)
        if resp.status_code in (200, 201):
            item = resp.json()
            item["id"] = item.get("id") or item.get("objectId")
            self._log(f"Created dataflow '{display_name}' → id={item['id']}")
            return item
        if resp.status_code == 202:
            location = resp.headers.get("Location") or resp.headers.get("location")
            if location:
                item = self._poll_lro(location)
                item["id"] = item.get("id") or item.get("objectId")
                return item

        # Already exists — find it and update its definition
        try:
            err = resp.json().get("errorCode", "")
        except Exception:
            err = ""
        if resp.status_code == 400 and "AlreadyInUse" in err:
            existing = self.find_item_by_name(workspace_id, display_name, "Dataflow")
            if existing:
                item_id = existing.get("id") or existing.get("objectId")
                print(f"  [info] Dataflow '{display_name}' already exists — updating definition.")
                upd_url = f"{FABRIC_API_BASE}/workspaces/{workspace_id}/dataflows/{item_id}/updateDefinition"
                upd_resp = self._request("POST", upd_url, payload=definition)
                if upd_resp.status_code == 202:
                    loc = upd_resp.headers.get("Location") or upd_resp.headers.get("location")
                    if loc:
                        self._poll_lro(loc)
                elif upd_resp.status_code not in (200, 201, 204):
                    self._raise_for_status(upd_resp)
                existing["id"] = item_id
                return existing

        self._raise_for_status(resp)
        return resp.json()

    # ------------------------------------------------------------------
    # Connections
    # ------------------------------------------------------------------

    def create_connection(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        Create a Fabric shareable connection.
        POST /v1/connections
        """
        url = f"{FABRIC_API_BASE}/connections"
        resp = self._request("POST", url, payload=payload)
        if resp.status_code in (200, 201):
            return resp.json()
        if resp.status_code == 202:
            location = resp.headers.get("Location") or resp.headers.get("location")
            if location:
                return self._poll_lro(location)
        try:
            body = resp.json()
        except Exception:
            body = {}
        err_code = body.get("errorCode", "")
        if resp.status_code == 400 and ("DuplicateConnectionName" in err_code or "already" in str(body).lower()):
            print(f"  [info] Connection already exists, skipping.")
            return body
        self._raise_for_status(resp)
        return resp.json()

    # ------------------------------------------------------------------
    # Folder helpers
    # ------------------------------------------------------------------

    def get_or_create_folder(self, workspace_id: str, folder_name: str) -> Optional[str]:
        """
        Return the folder ID for *folder_name* in the workspace, creating it
        if it doesn't exist.  Returns None if folder operations are not supported.
        """
        if not folder_name:
            return None
        # Try to create directly; if it already exists the API typically returns 409 with the id
        url = f"{FABRIC_API_BASE}/workspaces/{workspace_id}/folders"
        payload = {"displayName": folder_name}
        resp = self._request("POST", url, payload=payload)
        if resp.status_code in (200, 201):
            return resp.json().get("id")
        if resp.status_code == 409:
            # Folder already exists — list folders to find the id
            list_resp = self._request("GET", url)
            if list_resp.status_code == 200:
                for folder in list_resp.json().get("value", []):
                    if folder.get("displayName") == folder_name:
                        return folder.get("id")
        # Folder API not available in this workspace/tenant — fallback silently
        if resp.status_code in (404, 501, 405):
            print(
                f"  [info] Folder API not available for workspace {workspace_id}. "
                "Items will be created at root level."
            )
            return None
        # Non-critical failure — proceed without folder
        print(f"  [warn] Could not create/find folder '{folder_name}': HTTP {resp.status_code}")
        return None
