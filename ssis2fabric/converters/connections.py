"""
Convert SSIS Connection Managers to Fabric Connection creation payloads.

Fabric Connections REST API:  POST /v1/connections
"""
from typing import Any, Dict, List, Optional, Tuple

from ..models import SSISConnectionManager


# Fabric connection type mapping keyed on SSIS CreationName (lower-cased substring)
_CONN_TYPE_MAP = {
    "oledb":      ("SQL", "Sql"),
    "ado.net":    ("SQL", "Sql"),
    "sqlserver":  ("SQL", "Sql"),
    "file":       ("File", "FileShare"),
    "http":       ("HTTP", "AnonymousWeb"),
    "ftp":        ("FTP", "Anonymous"),
    "smtp":       ("SMTP", "Anonymous"),
    "excel":      ("SQL", "Sql"),
    "access":     ("SQL", "Sql"),
    "flatfile":   ("File", "FileShare"),
    "xml":        ("File", "FileShare"),
    "cache":      ("File", "FileShare"),
    "msmq":       ("Generic", "Anonymous"),
    "odbc":       ("SQL", "Sql"),
}


def _map_connection_type(ssis_type: str) -> Tuple[str, str]:
    """Return (fabric_connectivity_type, credential_type) for the given SSIS type."""
    key = ssis_type.lower()
    for fragment, mapping in _CONN_TYPE_MAP.items():
        if fragment in key:
            return mapping
    return ("SQL", "Sql")   # safe default


def build_connection_payload(cm: SSISConnectionManager, dummy: bool = False) -> Dict[str, Any]:
    """
    Build a Fabric Create Connection request payload for the given
    SSISConnectionManager.

    When *dummy* is True (or credentials are unknown) the payload uses
    placeholder/dummy credentials so the API call succeeds; the connection
    will need manual update afterwards.

    Fabric API reference:
      POST https://api.fabric.microsoft.com/v1/connections
    """
    conn_type, cred_type = _map_connection_type(cm.connection_type)

    # Determine connectivity details
    server = cm.server_name or "TODO_SERVER"
    database = cm.database_name or "TODO_DATABASE"
    file_path = cm.file_path or "TODO_PATH"
    url = cm.url or "TODO_URL"

    # Build connection details per connection type
    if conn_type == "SQL":
        connectivity_settings: Dict[str, Any] = {
            "connectionDetails": {
                "type": "SQL",
                "creationMethod": "SQL",
                "parameters": [
                    {"dataType": "Text", "name": "server", "value": server},
                    {"dataType": "Text", "name": "database", "value": database},
                ],
            },
        }
        credential_details: Dict[str, Any] = {
            "singleSignOnType": "None",
            "connectionEncryption": "NotEncrypted",
            "skipTestConnection": True,
            "credentials": {
                "credentialType": "Basic",
                "username": "TODO_USER",
                "password": "TODO_PASSWORD",
            },
        }
    elif conn_type == "File":
        connectivity_settings = {
            "connectivityType": "ShareableCloud",
            "gatewaySettings": {
                "gatewayObjectType": "None",
            },
            "connectionDetails": {
                "type": "File",
                "creationMethod": "File",
                "parameters": [
                    {"dataType": "Text", "name": "path", "value": file_path},
                ],
            },
        }
        credential_details = {
            "singleSignOnType": "None",
            "connectionEncryption": "NotEncrypted",
            "skipTestConnection": True,
            "credentials": {"credentialType": "Anonymous"},
        }
    elif conn_type == "HTTP":
        connectivity_settings = {
            "connectivityType": "ShareableCloud",
            "gatewaySettings": {
                "gatewayObjectType": "None",
            },
            "connectionDetails": {
                "type": "Web",
                "creationMethod": "WebApiAnonymous",
                "parameters": [
                    {"dataType": "Text", "name": "url", "value": url},
                ],
            },
        }
        credential_details = {
            "singleSignOnType": "None",
            "connectionEncryption": "NotEncrypted",
            "skipTestConnection": True,
            "credentials": {"credentialType": "Anonymous"},
        }
    else:
        # Generic fallback — dummy SQL connection
        connectivity_settings = {
            "connectionDetails": {
                "type": "SQL",
                "creationMethod": "SQL",
                "parameters": [
                    {"dataType": "Text", "name": "server", "value": server},
                    {"dataType": "Text", "name": "database", "value": database},
                ],
            },
        }
        credential_details = {
            "singleSignOnType": "None",
            "connectionEncryption": "NotEncrypted",
            "skipTestConnection": True,
            "credentials": {"credentialType": "Anonymous"},
        }

    return {
        "connectivityType": "ShareableCloud",
        "displayName": cm.name,
        "connectionDetails": connectivity_settings["connectionDetails"],
        "privacyLevel": "Organizational",
        "credentialDetails": credential_details,
    }


def convert_connections(
    connection_managers: List[SSISConnectionManager],
) -> List[Dict[str, Any]]:
    """
    Return a list of Fabric connection creation payloads for all given
    SSISConnectionManager instances.
    """
    payloads = []
    for cm in connection_managers:
        payload = build_connection_payload(cm, dummy=True)
        payloads.append({"ssis_name": cm.name, "ssis_id": cm.id, "payload": payload})
    return payloads
