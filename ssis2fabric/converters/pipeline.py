"""
Convert SSIS control flow to a Fabric Data Pipeline (pipeline-content.json).

Strategy
--------
* Walk the SSISPackage.tasks list and convert each task to a Fabric activity.
* SSISPrecedenceConstraints become activity dependsOn entries.
* Containers (ForEach, Sequence) map to their Fabric equivalents.
* Data Flow tasks generate a RefreshDataFlow activity referencing the
  Dataflow Gen2 that was created separately.
* Activities that cannot be fully represented use state=InActive so that
  the pipeline can still be created in Fabric and fixed up manually.

Fabric pipeline-content.json reference is embedded in the best-practices data.
"""
import base64
import json
import re
from typing import Any, Dict, List, Optional, Tuple

from ..models import (
    SSISDataFlow,
    SSISForEachLoop,
    SSISForLoop,
    SSISPackage,
    SSISPrecedenceConstraint,
    SSISSequenceContainer,
    SSISTask,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_INACTIVE = {"state": "InActive", "onInactiveMarkAs": "Succeeded"}

_DUMMY_CONNECTION_ID = "00000000-0000-0000-0000-000000000000"


def _safe(name: str) -> str:
    """Sanitize a name to be valid for Fabric activity names (keep spaces/underscores)."""
    cleaned = re.sub(r"[^\w\s\-]", "_", name)
    return cleaned[:100]


def _condition_for_value(value: int) -> str:
    """Map SSIS precedence constraint value integer to Fabric dependency condition."""
    return {0: "Succeeded", 1: "Failed", 2: "Completed"}.get(value, "Succeeded")


def _depends_on(
    task_id: str,
    precedence_constraints: List[SSISPrecedenceConstraint],
    id_to_name: Dict[str, str],
    name_to_id: Optional[Dict[str, str]] = None,
) -> List[Dict[str, Any]]:
    """Build the dependsOn list for a task.

    SSIS precedence constraint From/To values may be either:
      - A bare GUID (new-format packages)
      - A refId path like 'Package\\Task Name' (most packages)
    We handle both by extracting the last path segment as a name lookup.
    """
    deps = []
    name_to_id = name_to_id or {}

    def _resolve(key: str) -> Optional[str]:
        """Resolve a From/To value to a task id."""
        # Strip curly braces -> treat as GUID directly
        stripped = key.strip("{}").strip()
        if stripped in id_to_name:
            return stripped
        # RefId path: 'Package\Task Name' or 'Package\Container\Task Name'
        seg = re.split(r"[/\\]", key)[-1].strip()
        return name_to_id.get(seg)

    for pc in precedence_constraints:
        resolved_to = _resolve(pc.to_task)
        if resolved_to != task_id:
            continue
        resolved_from = _resolve(pc.from_task)
        from_name = id_to_name.get(resolved_from or "")
        if from_name:
            condition = _condition_for_value(pc.value)
            # Expression-only or ExpressionAndConstraint constraints that use
            # an expression are noted — Fabric has no direct expression guard;
            # the condition falls back to the constraint value.
            deps.append({
                "activity": _safe(from_name),
                "dependencyConditions": [condition],
            })
    return deps


# ---------------------------------------------------------------------------
# Individual task converters
# ---------------------------------------------------------------------------

def _convert_execute_sql(
    task: SSISTask,
    conn_id_map: Dict[str, str],
) -> Dict[str, Any]:
    """ExecuteSQL → Script activity (or SqlServerStoredProcedure if SP detected)."""
    sp_name = task.properties.get("stored_procedure_name")
    sql = task.properties.get("sql_statement", "")
    conn_id = conn_id_map.get(task.connection_ref or "", _DUMMY_CONNECTION_ID)

    if sp_name:
        return {
            "type": "SqlServerStoredProcedure",
            "typeProperties": {
                "storedProcedureName": sp_name,
                "storedProcedureParameters": {},
            },
            "externalReferences": {"connection": conn_id},
        }

    # Generic SQL → Script activity
    result_set = task.properties.get("result_set", "None")
    # ResultSet values: None=0/None, SingleRow=1, Full=2, XML=3
    return {
        "type": "Script",
        "typeProperties": {
            "scripts": [
                {
                    "type": "Query",
                    "text": sql or f"-- TODO: paste SQL from SSIS task '{task.name}'",
                }
            ],
            "logSettings": {"logDestination": "ActivityOutput"},
        },
        "externalReferences": {"connection": conn_id},
        "description": (
            f"Converted from SSIS Execute SQL Task. "
            f"Original ResultSet type: {result_set}. "
            + ("Result set capture must be wired manually via pipeline variables." if result_set not in ("0", "None", "") else "")
        ),
    }


def _convert_data_flow(
    task: SSISDataFlow,
    df_id_map: Dict[str, str],       # ssis data flow id → fabric dataflow guid
    workspace_id: str,
) -> Dict[str, Any]:
    """DataFlow task → RefreshDataFlow activity."""
    fabric_df_id = df_id_map.get(task.id, "TODO_DATAFLOW_ID")
    return {
        "type": "RefreshDataFlow",
        "typeProperties": {
            "dataflowId": fabric_df_id,
            "workspaceId": workspace_id,
            "notifyOption": "NoNotification",
            "dataflowType": "Dataflow",
        },
    }


def _convert_script_task(task: SSISTask) -> Dict[str, Any]:
    """ScriptTask → Script activity (inactive — needs manual porting)."""
    lang = task.properties.get("script_language", "CSharp")
    comment = (
        f"-- ScriptTask '{task.name}' (originally {lang})\n"
        f"-- TODO: port script logic here"
    )
    return {
        "type": "Script",
        **_INACTIVE,
        "typeProperties": {
            "scripts": [{"type": "Query", "text": comment}],
            "logSettings": {"logDestination": "ActivityOutput"},
        },
        "externalReferences": {"connection": _DUMMY_CONNECTION_ID},
    }


def _convert_send_mail(task: SSISTask) -> Dict[str, Any]:
    """SendMailTask → Office365Email activity."""
    p = task.properties
    to_addr  = p.get("to", "TODO@example.com")
    from_addr = p.get("from", "")
    cc_addr  = p.get("cc", "")
    bcc_addr = p.get("bcc", "")
    subject  = p.get("subject") or f"[SSIS migration] {task.name}"

    # Body: if MessageSourceType is not DirectInput the body came from a file or
    # variable at runtime — leave a TODO placeholder in that case.
    msg_type = str(p.get("message_source_type", "DirectInput")).strip()
    body_raw  = p.get("message", "")
    if msg_type in ("1", "FileConnection", "2", "Variable") or not body_raw:
        body = (
            f"TODO: original SSIS body source was '{msg_type}' — "
            f"replace with the actual message body or a dynamic expression."
        )
    else:
        body = body_raw

    # Priority (SSIS: Normal=0, High=1, Low=2)
    priority_raw = str(p.get("priority", "0")).strip()
    importance_map = {"0": "Normal", "Normal": "Normal",
                      "1": "High",   "High": "High",
                      "2": "Low",    "Low": "Low"}
    importance = importance_map.get(priority_raw, "Normal")

    type_props: Dict[str, Any] = {
        "to": to_addr,
        "subject": subject,
        "body": body,
        "operationType": "SendEmail",
        "importance": importance,
    }
    if from_addr:
        type_props["from"] = from_addr
    if cc_addr:
        type_props["cc"] = cc_addr
    if bcc_addr:
        type_props["bcc"] = bcc_addr
    attachments = p.get("attachments", "")
    if attachments:
        # SSIS allows semicolon-separated file paths; note them in description
        type_props["_attachments_note"] = (
            f"TODO: original SSIS attachment path(s): {attachments}"
        )

    return {
        "type": "Office365Email",
        **_INACTIVE,   # Wire up an Office 365 connection in the Fabric pipeline editor
        "typeProperties": type_props,
        "externalReferences": {"connection": _DUMMY_CONNECTION_ID},
    }


def _convert_execute_package(task: SSISTask, workspace_id: str) -> Dict[str, Any]:
    """ExecutePackageTask → ExecutePipeline activity (inactive — pipeline may not exist yet)."""
    pkg_name = task.properties.get("package_name", "TODO_PIPELINE_NAME")
    return {
        "type": "ExecutePipeline",
        **_INACTIVE,
        "typeProperties": {
            "pipeline": {
                "referenceName": pkg_name,
                "type": "PipelineReference",
            },
            "waitOnCompletion": True,
            "parameters": {},
        },
        "policy": {
            "secureInput": False,
            "secureOutput": False,
        },
    }


def _convert_execute_process(task: SSISTask) -> Dict[str, Any]:
    """ExecuteProcessTask → WebActivity (inactive — no direct equivalent)."""
    executable = task.properties.get("executable", "TODO")
    args = task.properties.get("arguments", "")
    return {
        "type": "WebActivity",
        **_INACTIVE,
        "typeProperties": {
            "relativeUrl": "TODO_URL",
            "method": "POST",
            "body": json.dumps({"executable": executable, "arguments": args}),
        },
        "externalReferences": {"connection": _DUMMY_CONNECTION_ID},
    }


def _convert_file_system(task: SSISTask) -> Dict[str, Any]:
    """FileSystemTask → Script activity (inactive — needs implementation)."""
    operation = task.properties.get("operation", "")
    return {
        "type": "Script",
        **_INACTIVE,
        "typeProperties": {
            "scripts": [
                {
                    "type": "Query",
                    "text": (
                        f"-- FileSystemTask '{task.name}'\n"
                        f"-- Operation: {operation}\n"
                        f"-- TODO: implement using Fabric lakehouse file operations"
                    ),
                }
            ],
            "logSettings": {"logDestination": "ActivityOutput"},
        },
        "externalReferences": {"connection": _DUMMY_CONNECTION_ID},
    }


def _convert_bulk_insert(task: SSISTask, conn_id_map: Dict[str, str]) -> Dict[str, Any]:
    """BulkInsertTask → Copy activity (inactive)."""
    conn_id = conn_id_map.get(task.connection_ref or "", _DUMMY_CONNECTION_ID)
    return {
        "type": "Copy",
        **_INACTIVE,
        "typeProperties": {
            "source": {
                "type": "SqlSource",
                "sqlReaderQuery": f"-- TODO: BulkInsert source for '{task.name}'",
            },
            "sink": {
                "type": "SqlSink",
                "writeBatchSize": 10000,
            },
        },
    }


def _convert_web_service_task(task: SSISTask, conn_id_map: Dict[str, str]) -> Dict[str, Any]:
    """
    WebServiceTask → WebActivity.

    SSIS Web Service Task calls SOAP/WSDL endpoints. Fabric's WebActivity
    makes raw HTTP calls, so the mapping is approximate:
      - Use the WSDL URL as the base URL (user must adjust to the SOAP endpoint)
      - Method is always POST for SOAP
      - SOAPAction header and envelope body must be filled in manually
    """
    p = task.properties
    wsdl    = p.get("wsdl_file", "")
    service = p.get("service", "")
    method  = p.get("web_method", "")
    output  = p.get("output", "")
    out_type = p.get("output_type", "")
    conn_id = conn_id_map.get(task.connection_ref or "", _DUMMY_CONNECTION_ID)

    # Build a placeholder SOAP envelope comment so the developer knows what to fill in
    soap_placeholder = (
        f"<!-- TODO: SOAP envelope for {service}.{method} -->\n"
        f"<soap:Envelope xmlns:soap=\"http://schemas.xmlsoap.org/soap/envelope/\">\n"
        f"  <soap:Body>\n"
        f"    <!-- insert {method} request parameters here -->\n"
        f"  </soap:Body>\n"
        f"</soap:Envelope>"
    )

    notes = [
        f"Converted from SSIS Web Service Task.",
        f"WSDL: {wsdl}" if wsdl else "WSDL: (not captured — check original package)",
        f"Service: {service}, Method: {method}" if service or method else "",
        f"Original output → {out_type}: {output}" if output else "",
        "Set the URL to the SOAP endpoint (not the WSDL URL).",
        "Add SOAPAction header and replace body with a real SOAP envelope.",
        "Capture the response via activity().output and store in a pipeline variable.",
    ]
    description = "  ".join(n for n in notes if n)

    return {
        "type": "WebActivity",
        **_INACTIVE,
        "typeProperties": {
            "url": wsdl or "TODO_SOAP_ENDPOINT_URL",
            "method": "POST",
            "headers": {
                "Content-Type": "text/xml; charset=utf-8",
                "SOAPAction": f"\"{method}\"" if method else "\"TODO_SOAP_ACTION\"",
            },
            "body": soap_placeholder,
        },
        "externalReferences": {"connection": conn_id},
        "description": description,
    }


def _convert_ftp_task(task: SSISTask, conn_id_map: Dict[str, str]) -> Dict[str, Any]:
    """
    FTPTask → Copy / Delete / Script activity (all InActive).

    Operation mapping
    -----------------
    Receive                → Copy   (FTP source  → Binary/ADLS sink)
    Send                   → Copy   (Binary source → FTP sink)
    DeleteRemoteFile       → Delete (FTP store)
    DeleteLocalFile        → Delete (local store)
    CreateRemoteDirectory
    RemoveRemoteDirectory
    RenameRemoteFile       → Script TODO (no direct Fabric equivalent)

    All activities are marked InActive — linked services and dataset paths
    must be wired up in Fabric before the pipeline can run.
    """
    p             = task.properties
    op            = (p.get("operation") or "").strip()
    local_path: str  = (
        p.get("local_var", "")  if p.get("is_local_path_variable")
        else p.get("local_path") or p.get("local_var") or "TODO:local_path"
    )
    remote_path: str = (
        p.get("remote_var", "") if p.get("is_remote_path_variable")
        else p.get("remote_path") or p.get("remote_var") or "TODO:remote_path"
    )
    recursive     = bool(p.get("recursive", False))
    overwrite     = bool(p.get("overwrite", False))
    transfer_type = "ASCII" if p.get("ascii_transfer") else "Binary"
    conn_id       = conn_id_map.get(task.connection_ref or "", _DUMMY_CONNECTION_ID)
    op_upper      = op.upper()

    # ---- Download: FTP → local/lakehouse ----
    if op_upper == "RECEIVE":
        return {
            "type": "Copy",
            **_INACTIVE,
            "description": (
                f"[FTP Download] Remote: {remote_path}  →  Local: {local_path}\n"
                f"Transfer: {transfer_type}  |  Recursive: {recursive}  |  Overwrite: {overwrite}\n"
                "TODO: configure FTP linked service + target dataset in Fabric."
            ),
            "typeProperties": {
                "source": {
                    "type": "BinarySource",
                    "storeSettings": {
                        "type": "FtpReadSettings",
                        "recursive": recursive,
                        "deleteFilesAfterCompletion": False,
                    },
                },
                "sink": {
                    "type": "BinarySink",
                    "storeSettings": {"type": "AzureBlobFSWriteSettings"},
                },
                "enableStaging": False,
            },
            "inputs":  [{"referenceName": f"FTP_Source_{_safe(task.name)}",  "type": "DatasetReference"}],
            "outputs": [{"referenceName": f"Local_Sink_{_safe(task.name)}",   "type": "DatasetReference"}],
        }

    # ---- Upload: local/lakehouse → FTP ----
    if op_upper == "SEND":
        return {
            "type": "Copy",
            **_INACTIVE,
            "description": (
                f"[FTP Upload] Local: {local_path}  →  Remote: {remote_path}\n"
                f"Transfer: {transfer_type}  |  Overwrite: {overwrite}\n"
                "TODO: configure local/ADLS linked service + FTP target dataset in Fabric."
            ),
            "typeProperties": {
                "source": {
                    "type": "BinarySource",
                    "storeSettings": {
                        "type": "AzureBlobFSReadSettings",
                        "recursive": False,
                    },
                },
                "sink": {
                    "type": "BinarySink",
                    "storeSettings": {
                        "type": "FtpWriteSettings",
                        "useTempFileRename": True,
                    },
                },
                "enableStaging": False,
            },
            "inputs":  [{"referenceName": f"Local_Source_{_safe(task.name)}", "type": "DatasetReference"}],
            "outputs": [{"referenceName": f"FTP_Sink_{_safe(task.name)}",     "type": "DatasetReference"}],
        }

    # ---- Delete remote or local file ----
    if op_upper in ("DELETEREMOTEFILE", "DELETELOCALFILE"):
        store_type = "FtpReadSettings" if op_upper == "DELETEREMOTEFILE" else "FileServerReadSettings"
        path_label = remote_path if op_upper == "DELETEREMOTEFILE" else local_path
        return {
            "type": "Delete",
            **_INACTIVE,
            "description": (
                f"[FTP {op}] Path: {path_label}\n"
                "TODO: configure linked service in Fabric."
            ),
            "typeProperties": {
                "dataset": {"referenceName": f"FTP_Delete_{_safe(task.name)}", "type": "DatasetReference"},
                "enableLogging": False,
                "storeSettings": {
                    "type": store_type,
                    "recursive": recursive,
                },
            },
        }

    # ---- Directory ops, rename, or unknown → Script placeholder ----
    return {
        "type": "Script",
        **_INACTIVE,
        "description": (
            f"[FTP {op or 'UnknownOperation'}] "
            f"Local: {local_path}  |  Remote: {remote_path}\n"
            "TODO: no direct Fabric equivalent — implement via HTTP activity or custom script."
        ),
        "typeProperties": {
            "scripts": [
                {
                    "type": "Query",
                    "text": (
                        f"-- FTP Task '{task.name}'\n"
                        f"-- Operation  : {op}\n"
                        f"-- Local path : {local_path}\n"
                        f"-- Remote path: {remote_path}\n"
                        f"-- FTP conn   : {task.connection_ref or 'N/A'}\n"
                        "-- TODO: implement this FTP operation"
                    ),
                }
            ],
            "logSettings": {"logDestination": "ActivityOutput"},
        },
        "externalReferences": {"connection": conn_id},
    }


def _convert_unknown_task(task: SSISTask) -> Dict[str, Any]:
    """Fallback: create an inactive Wait activity."""
    return {
        "type": "Wait",
        **_INACTIVE,
        "typeProperties": {
            "waitTimeInSeconds": 1,
        },
        # description carries the original task info as a reminder
    }


# ---------------------------------------------------------------------------
# Container converters
# ---------------------------------------------------------------------------

def _convert_for_loop(
    loop: SSISForLoop,
    precedence_constraints: List[SSISPrecedenceConstraint],
    conn_id_map: Dict[str, str],
    df_id_map: Dict[str, str],
    workspace_id: str,
) -> Dict[str, Any]:
    """
    SSISForLoop → Fabric Until activity.

    SSIS For Loop runs WHILE eval_expression is true.
    Fabric Until runs UNTIL expression is true, so we must negate.
    Because SSIS expression syntax differs from Fabric expressions, the
    converted expression is a TODO placeholder.
    """
    inner_activities = _convert_task_list(
        loop.tasks,
        loop.precedence_constraints,
        conn_id_map,
        df_id_map,
        workspace_id,
    )

    eval_expr = loop.eval_expression or ""
    assign_expr = loop.assign_expression or ""
    init_expr = loop.init_expression or ""

    # Build a descriptive placeholder condition with original SSIS expressions
    until_expression = (
        "@equals(1, 1) /* TODO: translate SSIS eval expression: "
        + (eval_expr or "(none)") + " "
        "Fabric Until runs UNTIL true; SSIS For Loop runs WHILE true — negate it. */"
    )

    description_parts = ["Converted from SSIS For Loop Container."]
    if init_expr:
        description_parts.append(f"InitExpression: {init_expr}")
    if eval_expr:
        description_parts.append(f"EvalExpression (loop while true): {eval_expr}")
    if assign_expr:
        description_parts.append(
            f"AssignExpression: {assign_expr} — "
            f"add a Set Variable activity inside the loop to implement this."
        )

    return {
        "type": "Until",
        **_INACTIVE,
        "typeProperties": {
            "expression": {"value": until_expression, "type": "Expression"},
            "activities": inner_activities,
            "timeout": "0.12:00:00",   # 12-hour safety guard; adjust as needed
        },
        "description": "  ".join(description_parts),
    }


def _convert_foreach_loop(
    loop: SSISForEachLoop,
    precedence_constraints: List[SSISPrecedenceConstraint],
    conn_id_map: Dict[str, str],
    df_id_map: Dict[str, str],
    workspace_id: str,
) -> Dict[str, Any]:
    """SSISForEachLoop → ForEach activity."""
    inner_activities = _convert_task_list(
        loop.tasks,
        loop.precedence_constraints,
        conn_id_map,
        df_id_map,
        workspace_id,
    )
    items_expr = loop.items_expression or "@createArray('TODO_ITEM')"
    return {
        "type": "ForEach",
        "typeProperties": {
            "items": {
                "value": items_expr,
                "type": "Expression",
            },
            "isSequential": True,
            "activities": inner_activities,
        },
    }


def _convert_sequence_container(
    seq: SSISSequenceContainer,
    conn_id_map: Dict[str, str],
    df_id_map: Dict[str, str],
    workspace_id: str,
) -> Dict[str, Any]:
    """SSISSequenceContainer is flattened — its inner tasks become nested inside an ExecutePipeline-like wrapper."""
    inner_activities = _convert_task_list(
        seq.tasks,
        seq.precedence_constraints,
        conn_id_map,
        df_id_map,
        workspace_id,
    )
    # Fabric doesn't have a direct "sequence" container, use an IfCondition that always runs true
    return {
        "type": "IfCondition",
        "typeProperties": {
            "expression": {"value": "@bool(1)", "type": "Expression"},
            "ifTrueActivities": inner_activities,
            "ifFalseActivities": [],
        },
    }


# ---------------------------------------------------------------------------
# Main task list conversion
# ---------------------------------------------------------------------------

def _convert_single_task(
    task: Any,
    precedence_constraints: List[SSISPrecedenceConstraint],
    id_to_name: Dict[str, str],
    name_to_id: Dict[str, str],
    conn_id_map: Dict[str, str],
    df_id_map: Dict[str, str],
    workspace_id: str,
) -> Dict[str, Any]:
    """Convert a single task/container to a Fabric activity dict."""
    depends = _depends_on(task.id, precedence_constraints, id_to_name, name_to_id)
    activity: Dict[str, Any] = {
        "name": _safe(task.name),
        "dependsOn": depends,
        "description": getattr(task, "description", None) or f"Converted from SSIS: {type(task).__name__}",
    }

    if isinstance(task, SSISDataFlow):
        activity.update(_convert_data_flow(task, df_id_map, workspace_id))
    elif isinstance(task, SSISForLoop):
        activity.update(
            _convert_for_loop(task, precedence_constraints, conn_id_map, df_id_map, workspace_id)
        )
    elif isinstance(task, SSISForEachLoop):
        activity.update(
            _convert_foreach_loop(task, precedence_constraints, conn_id_map, df_id_map, workspace_id)
        )
    elif isinstance(task, SSISSequenceContainer):
        activity.update(
            _convert_sequence_container(task, conn_id_map, df_id_map, workspace_id)
        )
    elif isinstance(task, SSISTask):
        tt = task.task_type
        if tt == "ExecuteSQL":
            activity.update(_convert_execute_sql(task, conn_id_map))
        elif tt == "DataFlow":
            # Shouldn't normally happen (SSISDataFlow objects are used instead), but handle
            activity.update({
                "type": "Wait",
                **_INACTIVE,
                "typeProperties": {"waitTimeInSeconds": 1},
            })
        elif tt == "Script":
            activity.update(_convert_script_task(task))
        elif tt == "SendMail":
            activity.update(_convert_send_mail(task))
        elif tt == "ExecutePackage":
            activity.update(_convert_execute_package(task, workspace_id))
        elif tt == "ExecuteProcess":
            activity.update(_convert_execute_process(task))
        elif tt == "FileSystem":
            activity.update(_convert_file_system(task))
        elif tt == "BulkInsert":
            activity.update(_convert_bulk_insert(task, conn_id_map))
        elif tt == "WebService":
            activity.update(_convert_web_service_task(task, conn_id_map))
        elif tt == "FTP":
            activity.update(_convert_ftp_task(task, conn_id_map))
        elif tt == "ForEachLoop":
            # Handled above via SSISForEachLoop class, but just in case
            activity.update({"type": "Wait", **_INACTIVE, "typeProperties": {"waitTimeInSeconds": 1}})
        else:
            activity.update(_convert_unknown_task(task))
            activity["description"] = (
                f"[MANUAL CONVERSION REQUIRED] Original SSIS task type: {task.task_type}"
            )
    else:
        activity.update(_convert_unknown_task(task))
        activity["description"] = "[MANUAL CONVERSION REQUIRED]"

    # Ensure 'type' is always present
    if "type" not in activity:
        activity["type"] = "Wait"
        activity.setdefault("typeProperties", {"waitTimeInSeconds": 1})

    # If the task was Disabled in SSIS, force InActive in Fabric regardless
    # of whether the specific converter already set a state.
    if getattr(task, "disabled", False):
        activity["state"] = "InActive"
        activity["onInactiveMarkAs"] = "Succeeded"
        existing_desc = activity.get("description") or ""
        disabled_note = "[Disabled in original SSIS package]"
        if disabled_note not in existing_desc:
            activity["description"] = (disabled_note + "  " + existing_desc).strip()

    return activity


def _convert_task_list(
    tasks: List[Any],
    precedence_constraints: List[SSISPrecedenceConstraint],
    conn_id_map: Dict[str, str],
    df_id_map: Dict[str, str],
    workspace_id: str,
) -> List[Dict[str, Any]]:
    """Convert a list of tasks with their precedence constraints to Fabric activities."""
    id_to_name: Dict[str, str] = {t.id: t.name for t in tasks}
    # Build reverse map for refId path resolution (task name → task id)
    name_to_id: Dict[str, str] = {t.name: t.id for t in tasks}
    activities = []
    for task in tasks:
        act = _convert_single_task(
            task, precedence_constraints, id_to_name, name_to_id,
            conn_id_map, df_id_map, workspace_id,
        )
        activities.append(act)
    return activities


# ---------------------------------------------------------------------------
# Build pipeline-content.json
# ---------------------------------------------------------------------------

def build_pipeline_content(
    package: SSISPackage,
    conn_id_map: Dict[str, str],   # ssis cm name/id → fabric connection guid
    df_id_map: Dict[str, str],     # ssis data flow id → fabric dataflow guid
    workspace_id: str,
) -> Dict[str, Any]:
    """
    Return the Fabric pipeline-content.json dict for the given SSISPackage.

    Parameters
    ----------
    package       : parsed SSIS package
    conn_id_map   : mapping of SSIS connection manager IDs/names → Fabric connection GUIDs
    df_id_map     : mapping of SSIS data flow task IDs → Fabric dataflow item GUIDs
    workspace_id  : target Fabric workspace GUID
    """
    activities = _convert_task_list(
        package.tasks,
        package.precedence_constraints,
        conn_id_map,
        df_id_map,
        workspace_id,
    )

    # SSIS User-namespace variables → Fabric pipeline parameters
    # System variables (PackageName, StartTime, etc.) are runtime-only; skip them.
    _type_map = {
        "String": "string", "DateTime": "string",   # Fabric has no native datetime param
        "Int32": "int", "Int64": "int", "Short": "int", "SByte": "int",
        "Byte": "int", "UInt16": "int",
        "Double": "float", "Decimal": "float",
        "Boolean": "bool",
        "Object": "object",
    }
    parameters: Dict[str, Any] = {}
    for var in package.variables:
        if var.namespace.lower() == "system":
            continue   # runtime-provided; no Fabric equivalent
        fabric_type = _type_map.get(var.data_type, "string")
        param: Dict[str, Any] = {"type": fabric_type}
        if var.value is not None:
            # Coerce default value to the right Python type
            try:
                if fabric_type == "int":
                    param["defaultValue"] = int(var.value)
                elif fabric_type == "float":
                    param["defaultValue"] = float(var.value)
                elif fabric_type == "bool":
                    param["defaultValue"] = var.value.lower() in ("true", "1", "yes")
                else:
                    param["defaultValue"] = var.value
            except (ValueError, AttributeError):
                param["defaultValue"] = var.value
        parameters[var.name] = param

    content: Dict[str, Any] = {
        "properties": {
            "description": (
                f"Converted from SSIS package '{package.name}'. "
                "Activities marked InActive require manual review."
            ),
            "activities": activities,
        }
    }
    if parameters:
        content["properties"]["parameters"] = parameters
    return content


def build_pipeline_definition(
    package: SSISPackage,
    conn_id_map: Dict[str, str],
    df_id_map: Dict[str, str],
    workspace_id: str,
) -> Dict[str, Any]:
    """
    Return the full Fabric item definition payload for creating a DataPipeline
    via the updateDefinition API.

    The payload has one part: 'pipeline-content.json' (base64-encoded).
    """
    pipeline_content = build_pipeline_content(package, conn_id_map, df_id_map, workspace_id)
    pipeline_json = json.dumps(pipeline_content, indent=2)
    pipeline_b64 = base64.b64encode(pipeline_json.encode("utf-8")).decode("ascii")

    return {
        "definition": {
            "parts": [
                {
                    "path": "pipeline-content.json",
                    "payload": pipeline_b64,
                    "payloadType": "InlineBase64",
                }
            ]
        }
    }
