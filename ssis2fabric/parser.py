"""
SSIS DTSX XML parser.

Parses a .dtsx file and produces an SSISPackage model.
"""
import re
import xml.etree.ElementTree as ET
from typing import Any, Dict, List, Optional, Tuple

from .models import (
    SSISConnectionManager,
    SSISDataFlow,
    SSISDataFlowComponent,
    SSISDataFlowPath,
    SSISForEachLoop,
    SSISForLoop,
    SSISPackage,
    SSISPrecedenceConstraint,
    SSISSequenceContainer,
    SSISTask,
    SSISVariable,
)

# ---------------------------------------------------------------------------
# XML namespace map used in SSIS DTSX files
# ---------------------------------------------------------------------------
NS = {
    "DTS": "www.microsoft.com/SqlServer/Dts",
    "SQLTask": "www.microsoft.com/sqlserver/dts/tasks/sqltask",
    "ForEachFileEnumerator": "www.microsoft.com/sqlserver/dts/tasks/foreachfileenumerator",
    "component": "www.microsoft.com/SqlServer/Dts/Pipeline",
}

# Prefixed namespace helpers
_DTS = "{www.microsoft.com/SqlServer/Dts}"
_PIPELINE = "{www.microsoft.com/SqlServer/Dts/Pipeline}"


def _prop_text(elem: ET.Element, name: str) -> Optional[str]:
    """Read value from <DTS:Property DTS:Name="name"> child (old DTSX format)."""
    for child in elem:
        if child.tag != f"{_DTS}Property":
            continue
        if child.get(f"{_DTS}Name") == name or child.get("DTS:Name") == name:
            return (child.text or "").strip() or None
    return None


def _attr(elem: ET.Element, *names: str) -> Optional[str]:
    """
    Return the first matching value, trying in order:
      1. XML attribute DTS:<name>  (new format)
      2. XML attribute <name>      (bare)
      3. <DTS:Property DTS:Name="<name>"> child text  (old format)
    """
    for name in names:
        v = elem.get(f"{_DTS}{name}")
        if v is not None:
            return v.strip()
        v = elem.get(name)
        if v is not None:
            return v.strip()
    # Fallback: property-element style (old format)
    for name in names:
        v = _prop_text(elem, name)
        if v is not None:
            return v
    return None


def _child_text(elem: ET.Element, tag: str) -> Optional[str]:
    child = elem.find(f"{_DTS}{tag}")
    if child is not None and child.text:
        return child.text.strip()
    return None


def _clean_id(raw: Optional[str]) -> str:
    """Strip braces from GUID-style IDs."""
    if not raw:
        return ""
    return raw.strip("{}")


def _task_type_from_ref(creation_name: str) -> str:
    """Derive a simple task type key from DTS:CreationName / ObjectType."""
    mapping = {
        "Microsoft.ExecutePackageTask": "ExecutePackage",
        "Microsoft.ExecuteSQLTask": "ExecuteSQL",
        "Microsoft.DataFlowTask": "DataFlow",
        "Microsoft.ScriptTask": "Script",
        "Microsoft.SendMailTask": "SendMail",
        "Microsoft.FileSystemTask": "FileSystem",
        "Microsoft.ExecuteProcessTask": "ExecuteProcess",
        "Microsoft.BulkInsertTask": "BulkInsert",
        "Microsoft.FTPTask": "FTP",
        "Microsoft.WebServiceTask": "WebService",
        "Microsoft.XMLTask": "XMLTask",
        "Microsoft.ExpressionTask": "Expression",
        "Microsoft.TransferDatabaseTask": "TransferDatabase",
        "STOCK:FOREACHLOOP": "ForEachLoop",
        "STOCK:SEQUENCE": "Sequence",
        "STOCK:FORLOOP": "ForLoop",
        "DTS.Pipeline.2": "DataFlow",
        "DTS.Pipeline.1": "DataFlow",
        "MSDTS.Pipeline": "DataFlow",
        # old-format package root – skip
        "MSDTS.Package.1": "Package",
    }
    for key, val in mapping.items():
        if key.lower() in creation_name.lower():
            return val
    # Heuristics
    cn = creation_name.lower()
    if "pipeline" in cn:
        return "DataFlow"
    if "foreachloop" in cn or "foreach" in cn:
        return "ForEachLoop"
    if "sequence" in cn:
        return "Sequence"
    if "sql" in cn:
        return "ExecuteSQL"
    if "script" in cn:
        return "Script"
    if "web" in cn or "wsdl" in cn or "soap" in cn:
        return "WebService"
    return "Unknown"


# ---------------------------------------------------------------------------
# Connection manager parsing
# ---------------------------------------------------------------------------

def _parse_connection_manager(cm_elem: ET.Element) -> SSISConnectionManager:
    name = _attr(cm_elem, "ObjectName") or "UnknownConnection"
    cm_id = _clean_id(_attr(cm_elem, "DTSID"))
    conn_type = _attr(cm_elem, "CreationName") or "OLEDB"

    # Pull connection string / object properties from ObjectData
    obj_data = cm_elem.find(f"{_DTS}ObjectData")
    conn_str = None
    server_name = None
    database_name = None
    file_path = None
    url = None
    props: Dict[str, Any] = {}

    if obj_data is not None:
        # Generic property bag
        for prop in obj_data.iter(f"{_DTS}Property"):
            pname = _attr(prop, "Name") or ""
            props[pname] = prop.text or ""

        # OLEDB / ADO.NET connection string element
        conn_mgr_elem = obj_data.find(f".//{_DTS}ConnectionManager")
        if conn_mgr_elem is not None:
            # Try attribute first (new format), then property element (old format)
            conn_str = (
                conn_mgr_elem.get(f"{_DTS}ConnectionString")
                or conn_mgr_elem.get("ConnectionString")
                or _prop_text(conn_mgr_elem, "ConnectionString")
            )
            if conn_str:
                # Try to extract server / database from connection string
                m = re.search(r"Data Source=([^;]+)", conn_str, re.I)
                if m:
                    server_name = m.group(1)
                m = re.search(r"Initial Catalog=([^;]+)", conn_str, re.I)
                if m:
                    database_name = m.group(1)

        # FILE connection
        file_elem = obj_data.find(f".//{_DTS}FileConnectionManager")
        if file_elem is not None:
            file_path = _attr(file_elem, "FileUsageType") or props.get("ConnectionString")

        # HTTP connection
        http_elem = obj_data.find(f".//{_DTS}HttpConnectionManager")
        if http_elem is not None:
            url = _attr(http_elem, "ServerURL")

    return SSISConnectionManager(
        id=cm_id,
        name=name,
        connection_type=conn_type,
        connection_string=conn_str,
        server_name=server_name,
        database_name=database_name,
        file_path=file_path,
        url=url,
        properties=props,
    )


# ---------------------------------------------------------------------------
# Variable parsing
# ---------------------------------------------------------------------------

def _parse_variables(pkg_elem: ET.Element) -> List[SSISVariable]:
    variables = []
    for var_elem in pkg_elem.findall(f".//{_DTS}Variable"):
        var_id = _clean_id(_attr(var_elem, "DTSID"))
        var_name = _attr(var_elem, "ObjectName") or "Var"
        namespace = _attr(var_elem, "Namespace") or "User"
        data_type_raw = _attr(var_elem, "DataType") or "8"
        dt_map = {
            "3": "Int32", "5": "Double", "7": "DateTime", "8": "String",
            "11": "Boolean", "14": "Decimal", "16": "SByte", "17": "Byte",
            "18": "Short", "19": "UInt16", "20": "Int64",
        }
        data_type = dt_map.get(data_type_raw, "String")
        value = None
        val_elem = var_elem.find(f"{_DTS}VariableValue")
        if val_elem is not None and val_elem.text:
            value = val_elem.text.strip()
        variables.append(SSISVariable(id=var_id, name=var_name, namespace=namespace,
                                      data_type=data_type, value=value))
    return variables


# ---------------------------------------------------------------------------
# Data flow (pipeline task) parsing
# ---------------------------------------------------------------------------

def _parse_data_flow(task_elem: ET.Element, task_id: str, task_name: str) -> SSISDataFlow:
    """Parse the nested Pipeline XML inside a Data Flow task."""
    components: List[SSISDataFlowComponent] = []
    paths: List[SSISDataFlowPath] = []

    obj_data = task_elem.find(f"{_DTS}ObjectData")
    if obj_data is None:
        return SSISDataFlow(id=task_id, name=task_name)

    pipeline_wrapper = obj_data.find(f".//{_PIPELINE}pipeline")
    if pipeline_wrapper is None:
        # Try without namespace
        pipeline_wrapper = obj_data.find(".//pipeline")

    if pipeline_wrapper is not None:
        components_elem = pipeline_wrapper.find(f"{_PIPELINE}components")
        if components_elem is None:
            components_elem = pipeline_wrapper.find("components")
        if components_elem is not None:
            for comp in components_elem:
                comp_id = _clean_id(comp.get("id") or "")
                comp_name = comp.get("name") or f"Component_{comp_id}"
                comp_class = comp.get("componentClassID") or comp.get("componentClassId") or ""
                conn_ref = None
                sql_cmd = None
                table_name = None
                props: Dict[str, Any] = {}
                input_cols: List[Dict] = []
                output_cols: List[Dict] = []

                # Properties
                for prop in comp.iter("property"):
                    pname = prop.get("name") or ""
                    props[pname] = prop.text or ""
                    if "SqlCommand" in pname:
                        sql_cmd = prop.text
                    if "TableName" in pname or "OpenRowset" in pname:
                        table_name = prop.text

                # Connection managers – tag may be 'connectionManager' (new) or 'connection' (old)
                for tag_name in ("connectionManager", "connection"):
                    for cm_ref in comp.iter(tag_name):
                        cr = (
                            cm_ref.get("connectionManagerID")
                            or cm_ref.get("connectionManagerId")
                            or cm_ref.get("name")
                        )
                        if cr:
                            conn_ref = _clean_id(cr)
                            break
                    if conn_ref:
                        break

                # Output columns
                for out_col in comp.iter("outputColumn"):
                    output_cols.append({
                        "id": out_col.get("id"),
                        "name": out_col.get("name"),
                        "dataType": out_col.get("dataType"),
                    })

                # Input columns
                for in_col in comp.iter("inputColumn"):
                    input_cols.append({
                        "id": in_col.get("id"),
                        "name": in_col.get("name"),
                        "upstreamComponentId": in_col.get("sourceId") or in_col.get("cachedName"),
                    })

                components.append(SSISDataFlowComponent(
                    id=comp_id, name=comp_name, component_class=comp_class,
                    connection_ref=conn_ref, sql_command=sql_cmd, table_name=table_name,
                    properties=props, input_columns=input_cols, output_columns=output_cols,
                ))

        paths_elem = pipeline_wrapper.find(f"{_PIPELINE}paths")
        if paths_elem is None:
            paths_elem = pipeline_wrapper.find("paths")
        if paths_elem is not None:
            for path in paths_elem:
                path_id = path.get("id") or ""
                path_name = path.get("name") or f"Path_{path_id}"
                start_id = path.get("startId") or ""
                end_id = path.get("endId") or ""
                paths.append(SSISDataFlowPath(
                    id=path_id, name=path_name,
                    start_id=start_id, end_id=end_id,
                ))

    return SSISDataFlow(id=task_id, name=task_name, components=components, paths=paths)


# ---------------------------------------------------------------------------
# Generic task parsing
# ---------------------------------------------------------------------------

def _parse_task(exec_elem: ET.Element) -> Any:
    """Parse a single DTS:Executable element into a task model."""
    task_name = _attr(exec_elem, "ObjectName") or "Task"
    task_id = _clean_id(_attr(exec_elem, "DTSID"))
    # CreationName may be an attribute OR a child property element;
    # in old format DTS:ExecutableType attribute also carries the task type.
    creation_name = (
        _attr(exec_elem, "CreationName")
        or exec_elem.get(f"{_DTS}ExecutableType")
        or exec_elem.get("DTS:ExecutableType")
        or ""
    )
    description = _attr(exec_elem, "Description")
    task_type = _task_type_from_ref(creation_name)
    disabled = (_attr(exec_elem, "Disabled") or "").lower() in ("true", "1")

    # Skip package-root executables used as wrappers in old format
    if task_type == "Package":
        return SSISTask(
            id=task_id, name=task_name, task_type="Unknown",
            description=description, connection_ref=None, properties={},
        )

    # --- For Loop Container ---
    if task_type == "ForLoop":
        inner_tasks = []
        inner_pcs = []
        for child_exec in exec_elem.findall(f"{_DTS}Executables/{_DTS}Executable"):
            inner_tasks.append(_parse_task(child_exec))
        if not inner_tasks:
            for child_exec in exec_elem.findall(f"{_DTS}Executable"):
                inner_tasks.append(_parse_task(child_exec))
        for pc in exec_elem.findall(
            f"{_DTS}PrecedenceConstraints/{_DTS}PrecedenceConstraint"
        ):
            inner_pcs.append(_parse_precedence_constraint(pc))
        return SSISForLoop(
            id=task_id,
            name=task_name,
            init_expression=_attr(exec_elem, "InitExpression") or None,
            eval_expression=_attr(exec_elem, "EvalExpression") or None,
            assign_expression=_attr(exec_elem, "AssignExpression") or None,
            tasks=inner_tasks,
            precedence_constraints=inner_pcs,
            description=description,
            disabled=disabled,
        )

    # --- ForEach Loop ---
    if task_type == "ForEachLoop":
        inner_tasks = []
        inner_pcs = []
        # New format: <DTS:Executables> wrapper
        for child_exec in exec_elem.findall(f"{_DTS}Executables/{_DTS}Executable"):
            inner_tasks.append(_parse_task(child_exec))
        # Old format: direct children
        if not inner_tasks:
            for child_exec in exec_elem.findall(f"{_DTS}Executable"):
                inner_tasks.append(_parse_task(child_exec))
        for pc in exec_elem.findall(
            f"{_DTS}PrecedenceConstraints/{_DTS}PrecedenceConstraint"
        ):
            inner_pcs.append(_parse_precedence_constraint(pc))

        enum_type = "ItemEnumerator"
        items_expr = None
        var_name = None

        fe_data = exec_elem.find(f"{_DTS}ObjectData")
        if fe_data is not None:
            foreach_enum = fe_data.find(f".//{_DTS}ForEachEnumerator")
            if foreach_enum is not None:
                et_name = _attr(foreach_enum, "CreationName") or ""
                if "File" in et_name:
                    enum_type = "File"
                elif "ADO" in et_name:
                    enum_type = "ADO"
                elif "NodeList" in et_name:
                    enum_type = "NodeList"
                elif "Item" in et_name:
                    enum_type = "Item"

        # Variable mappings
        for vm in exec_elem.findall(
            f".//{_DTS}ForEachVariableMapping"
        ):
            var_name = _attr(vm, "VariableName") or var_name

        props: Dict[str, Any] = {
            "creationName": creation_name,
            "description": description or "",
        }
        return SSISForEachLoop(
            id=task_id, name=task_name, enumerator_type=enum_type,
            items_expression=items_expr, variable_name=var_name,
            tasks=inner_tasks, precedence_constraints=inner_pcs,
            properties=props, disabled=disabled,
        )

    # --- Sequence Container ---
    if task_type == "Sequence":
        inner_tasks = []
        inner_pcs = []
        # New format: <DTS:Executables> wrapper
        for child_exec in exec_elem.findall(f"{_DTS}Executables/{_DTS}Executable"):
            inner_tasks.append(_parse_task(child_exec))
        # Old format: direct children
        if not inner_tasks:
            for child_exec in exec_elem.findall(f"{_DTS}Executable"):
                inner_tasks.append(_parse_task(child_exec))
        for pc in exec_elem.findall(
            f"{_DTS}PrecedenceConstraints/{_DTS}PrecedenceConstraint"
        ):
            inner_pcs.append(_parse_precedence_constraint(pc))
        return SSISSequenceContainer(
            id=task_id, name=task_name,
            tasks=inner_tasks, precedence_constraints=inner_pcs,
            disabled=disabled,
        )

    # --- Data Flow Task ---
    if task_type == "DataFlow":
        return _parse_data_flow(exec_elem, task_id, task_name)

    # --- Generic task ---
    props: Dict[str, Any] = {
        "creationName": creation_name,
        "description": description or "",
    }
    connection_ref = None

    obj_data = exec_elem.find(f"{_DTS}ObjectData")
    if obj_data is not None:
        # ExecuteSQL specific
        sql_task_node = obj_data.find(
            f".//{{{NS['SQLTask']}}}SqlTaskData"
        )
        if sql_task_node is not None:
            sql = sql_task_node.get(f"{{{NS['SQLTask']}}}SqlStatementSource")
            if sql:
                props["sql_statement"] = sql
            conn = sql_task_node.get(f"{{{NS['SQLTask']}}}Connection")
            if conn:
                connection_ref = _clean_id(conn)
            sp = sql_task_node.get(f"{{{NS['SQLTask']}}}StoredProcedureName")
            if sp:
                props["stored_procedure_name"] = sp
            result_set = sql_task_node.get(f"{{{NS['SQLTask']}}}ResultSet")
            if result_set:
                props["result_set"] = result_set

        # Script task
        script_elem = obj_data.find(f".//{_DTS}ScriptProject")
        if script_elem is not None:
            props["script_language"] = _attr(script_elem, "Language") or "CSharp"

        # SendMail
        mail_elem = obj_data.find(f".//{_DTS}SendMailTask")
        if mail_elem is None:
            mail_elem = obj_data.find(".//SendMailData")
        if mail_elem is not None:
            props["to"] = mail_elem.get("To") or ""
            props["from"] = mail_elem.get("From") or ""
            props["cc"] = mail_elem.get("CC") or ""
            props["bcc"] = mail_elem.get("BCC") or ""
            props["subject"] = mail_elem.get("Subject") or ""
            # MessageSourceType: DirectInput=0, FileConnection=1, Variable=2
            props["message_source_type"] = mail_elem.get("MessageSourceType") or "DirectInput"
            props["message"] = mail_elem.get("MessageSource") or mail_elem.get("MessageBody") or ""
            # Priority: Normal=0, High=1, Low=2
            props["priority"] = mail_elem.get("Priority") or "Normal"
            props["attachments"] = mail_elem.get("FileAttachments") or ""
        # Also pick up SendMail properties stored as DTS:Property elements
        # (some versions of the DTSX schema embed them this way)
        for prop in obj_data.iter(f"{_DTS}Property"):
            pname = _attr(prop, "Name") or ""
            if pname in ("To", "From", "CC", "BCC", "Subject", "MessageSource",
                         "MessageSourceType", "Priority", "FileAttachments"):
                key_map = {
                    "To": "to", "From": "from", "CC": "cc", "BCC": "bcc",
                    "Subject": "subject", "MessageSource": "message",
                    "MessageSourceType": "message_source_type",
                    "Priority": "priority", "FileAttachments": "attachments",
                }
                mapped = key_map.get(pname, pname.lower())
                props.setdefault(mapped, prop.text or "")

        # Web Service Task
        ws_elem = obj_data.find(f".//{_DTS}WebServiceTask")
        if ws_elem is None:
            ws_elem = obj_data.find(".//WebServiceTaskData")
        if ws_elem is not None:
            props["wsdl_file"] = ws_elem.get("WSDLFile") or ""
            props["service"]   = ws_elem.get("Service") or ""
            props["web_method"]= ws_elem.get("WebMethod") or ""
            props["output_type"] = ws_elem.get("OutputType") or ""  # Variable or File
            props["output"]    = ws_elem.get("Output") or ""
            # Connection ref for the HTTP connection manager
            http_conn = ws_elem.get("Connection") or ws_elem.get("HTTPConnection")
            if http_conn and not connection_ref:
                connection_ref = _clean_id(http_conn)

        # Execute Process
        exec_proc = obj_data.find(f".//{_DTS}ExecuteProcessData")
        if exec_proc is not None:
            props["executable"] = exec_proc.get("Executable") or ""
            props["arguments"] = exec_proc.get("Arguments") or ""

        # Execute Package task
        exec_pkg = obj_data.find(f".//{_DTS}ExecutePackageTask")
        if exec_pkg is not None:
            pkg_ref = _child_text(exec_pkg, "PackageName") or ""
            props["package_name"] = pkg_ref

        # File System task
        fs_task = obj_data.find(f".//{_DTS}FileSystemData")
        if fs_task is not None:
            props["operation"] = fs_task.get("TaskOperationType") or ""

        # FTP Task  (namespace: www.microsoft.com/sqlserver/dts/tasks/ftptask)
        _FTP_NS = "www.microsoft.com/sqlserver/dts/tasks/ftptask"
        ftp_elem = None
        for _el in obj_data.iter():
            if _el.tag.endswith("}FTPTaskData") or _el.tag == "FTPTaskData":
                ftp_elem = _el
                break
        if ftp_elem is not None:
            def _ftp(attr: str) -> str:  # type: ignore[misc]
                return ftp_elem.get(f"{{{_FTP_NS}}}{attr}") or ftp_elem.get(attr) or ""  # type: ignore[union-attr]
            props["operation"]              = _ftp("Operation")
            props["local_path"]             = _ftp("LocalPath")
            props["remote_path"]            = _ftp("RemotePath")
            props["local_var"]              = _ftp("LocalVariable")
            props["remote_var"]             = _ftp("RemoteVariable")
            props["is_local_path_variable"] = _ftp("IsLocalPathVariable").lower() == "true"
            props["is_remote_path_variable"]= _ftp("IsRemotePathVariable").lower() == "true"
            props["overwrite"]              = _ftp("OverwriteFileAtDestination").lower() == "true"
            props["ascii_transfer"]         = _ftp("IsTransferTypeASCII").lower() == "true"
            props["recursive"]              = _ftp("IsRecursive").lower() == "true"
            ftp_conn = _ftp("Connection")
            if ftp_conn and not connection_ref:
                connection_ref = _clean_id(ftp_conn)

        # Generic property bag
        for prop in obj_data.iter(f"{_DTS}Property"):
            pname = _attr(prop, "Name") or ""
            props[pname] = prop.text or ""

    return SSISTask(
        id=task_id, name=task_name, task_type=task_type,
        description=description, connection_ref=connection_ref,
        properties=props, disabled=disabled,
    )


# ---------------------------------------------------------------------------
# Precedence constraint parsing
# ---------------------------------------------------------------------------

def _parse_precedence_constraint(pc_elem: ET.Element) -> SSISPrecedenceConstraint:
    from_task = _clean_id(_attr(pc_elem, "From"))
    to_task = _clean_id(_attr(pc_elem, "To"))
    eval_op_raw = _attr(pc_elem, "EvalOp") or "0"
    eval_map = {
        "0": "Constraint",
        "1": "Expression",
        "2": "ExpressionAndConstraint",
        "3": "ExpressionOrConstraint",
    }
    eval_op = eval_map.get(eval_op_raw, "Constraint")
    value_raw = _attr(pc_elem, "Value") or "0"
    value_map = {"0": 0, "1": 1, "2": 2}   # 0=Success, 1=Failure, 2=Completion
    value = value_map.get(value_raw, 0)
    expression = _attr(pc_elem, "Expression")
    return SSISPrecedenceConstraint(
        from_task=from_task, to_task=to_task,
        eval_op=eval_op, value=value, expression=expression,
    )


# ---------------------------------------------------------------------------
# Top-level parser entry point
# ---------------------------------------------------------------------------

def parse_dtsx(path: str) -> SSISPackage:
    """Parse a .dtsx file at *path* and return a populated SSISPackage."""
    tree = ET.parse(path)
    root = tree.getroot()

    pkg_name = _attr(root, "ObjectName") or "Package"
    pkg_id = _clean_id(_attr(root, "DTSID"))
    description = _attr(root, "Description")

    # ---- Connection Managers ----
    connection_managers: List[SSISConnectionManager] = []
    seen_cm_ids: set = set()

    def _add_cm(cm: SSISConnectionManager) -> None:
        key = cm.id or cm.name
        if key not in seen_cm_ids:
            seen_cm_ids.add(key)
            connection_managers.append(cm)

    # New format: wrapped in <DTS:ConnectionManagers>
    for cm in root.findall(f"{_DTS}ConnectionManagers/{_DTS}ConnectionManager"):
        _add_cm(_parse_connection_manager(cm))
    # Old format: direct children of root
    for cm in root.findall(f"{_DTS}ConnectionManager"):
        _add_cm(_parse_connection_manager(cm))

    # ---- Variables ----
    variables = _parse_variables(root)

    # ---- Control Flow tasks (top-level executables only) ----
    tasks = []

    def _add_task(t: Any) -> None:
        # Drop package-level wrapper tasks (old format MSDTS.Package.1)
        if getattr(t, "task_type", None) == "Package":
            return
        if getattr(t, "task_type", None) == "Unknown" and not getattr(t, "id", None):
            return
        tasks.append(t)

    # New format: <DTS:Executables> wrapper
    for exec_elem in root.findall(f"{_DTS}Executables/{_DTS}Executable"):
        _add_task(_parse_task(exec_elem))
    # Old format: direct children of root
    for exec_elem in root.findall(f"{_DTS}Executable"):
        _add_task(_parse_task(exec_elem))

    # ---- Precedence Constraints (top-level) ----
    precedence_constraints: List[SSISPrecedenceConstraint] = []
    # New format
    for pc in root.findall(
        f"{_DTS}PrecedenceConstraints/{_DTS}PrecedenceConstraint"
    ):
        precedence_constraints.append(_parse_precedence_constraint(pc))
    # Old format: direct children
    for pc in root.findall(f"{_DTS}PrecedenceConstraint"):
        precedence_constraints.append(_parse_precedence_constraint(pc))

    # ---- Collect all DataFlow objects from tasks list ----
    data_flows: List[SSISDataFlow] = []

    def collect_data_flows(task_list: List[Any]) -> None:
        for t in task_list:
            if isinstance(t, SSISDataFlow):
                data_flows.append(t)
            elif isinstance(t, SSISForEachLoop):
                collect_data_flows(t.tasks)
            elif isinstance(t, SSISForLoop):
                collect_data_flows(t.tasks)
            elif isinstance(t, SSISSequenceContainer):
                collect_data_flows(t.tasks)

    collect_data_flows(tasks)

    return SSISPackage(
        name=pkg_name,
        id=pkg_id,
        description=description,
        connection_managers=connection_managers,
        tasks=tasks,
        precedence_constraints=precedence_constraints,
        data_flows=data_flows,
        variables=variables,
    )
