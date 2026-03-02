"""
Convert SSIS Data Flow tasks to Fabric Dataflow Gen2 (Power Query) definitions.

The Fabric Dataflow Gen2 definition uses Mashup (Power Query M) expressions.
We produce a best-effort M document capturing sources, transformations, and
destinations identified in the SSIS Data Flow.  Activities that cannot be
fully translated are annotated with TODO comments.
"""
import base64
import json
import re
import uuid
from typing import Any, Dict, List, Optional, Tuple

from ..models import SSISDataFlow, SSISDataFlowComponent


# ---------------------------------------------------------------------------
# Component class → category mapping
# ---------------------------------------------------------------------------
_SOURCE_CLASSES = {
    "microsoft.oledbsource",
    "microsoft.sqlserversource",
    "microsoft.flatfilesource",
    "microsoft.excelsource",
    "microsoft.xmlsource",
    "microsoft.odata",
    "microsoft.adosource",
    "microsoft.adoneutils.sqlserver",
    "dtspipeline.oledbsource",
}
_DEST_CLASSES = {
    "microsoft.oledbdestination",
    "microsoft.sqlserverdestination",
    "microsoft.flatfiledestination",
    "microsoft.exceldestination",
    "microsoft.sqldestination",
    "dtspipeline.oledbdestination",
}
_TRANSFORM_CLASSES = {
    # Match on substrings of the (lowercased) componentClassID
    "derivedcolumn": "DerivedColumn",
    "derived column": "DerivedColumn",
    "aggregatetransform": "Aggregate",
    "aggregate": "Aggregate",
    "lookup": "Lookup",
    "sort": "Sort",
    "conditionalsplit": "ConditionalSplit",
    "conditional split": "ConditionalSplit",
    "mergejoin": "MergeJoin",
    "merge join": "MergeJoin",
    "unionall": "UnionAll",
    "union all": "UnionAll",
    "dataconversion": "DataConversion",
    "data conversion": "DataConversion",
    "rowcount": "RowCount",
    "multicast": "Multicast",
    "pivot": "Pivot",
    "unpivot": "Unpivot",
    "charactermap": "CharacterMap",
    "fuzzygrouping": "FuzzyGrouping",
    "fuzzylookup": "FuzzyLookup",
    "termextraction": "TermExtraction",
    "termlookup": "TermLookup",
    "slowlychangingdimension": "SlowlyChangingDimension",
    "slowly changing": "SlowlyChangingDimension",
    "script": "Script",
    "scriptcomponent": "Script",
}


# Well-known SSIS component GUIDs (old DTSX format uses GUIDs instead of class names)
_GUID_CATEGORY: Dict[str, str] = {
    # Sources
    "2C0A8BE5-1EDC-4353-A0EF-B778599C65A0": "source",   # OLE DB Source
    "BCEFE59B-6819-47F7-A125-63753B3D9F2D": "source",   # Flat File Source
    "90C7770B-DE7C-435E-880E-E718C92C0573": "source",   # Excel Source
    "5ACD952A-F16A-41D8-A681-713640837664": "source",   # ADO NET Source
    "874F7595-FB5F-40FF-96AF-FBFF8250E3EF": "source",   # XML Source
    "C9A5FD97-818E-40B5-ACD5-B03EF9E85F90": "source",   # Raw File Source
    # Destinations
    "E2568105-9550-4F71-A638-B7F2D6C7B3B7": "destination",   # OLE DB Destination
    "4963CAED-CB38-4146-96F0-5910342FF3B9": "destination",   # Excel Destination
    "D658C424-8CF0-44C0-B2E7-D05D2837D939": "destination",   # Flat File Destination
    "61DFB849-E9FB-4A0E-985D-0BCB0DB7AEBF": "destination",   # ADO NET Destination
    "E4A3D6EE-8F0D-4F9C-BF6A-295A5B0FA60D": "destination",   # Raw File Destination
    "34D34E3F-9CA4-4FAF-B2EF-4408AC1A21EC": "destination",   # Recordset Destination
    # Transforms
    "9CF90BF0-5BCC-4C63-B91D-1F322DC12C26": "transform",   # Derived Column
    "F02464D7-9F5B-4C45-8A42-E23B42A5D3FD": "transform",   # Aggregate
    "E4B61575-C0E3-4B5F-BAAF-4BBF18B3E21D": "transform",   # Sort
    "2932025B-AB99-40F6-B5B8-783A73F80E24": "transform",   # Lookup
    "B4A5D501-1D4A-4855-A856-B7B4A2CEF7B3": "transform",   # Conditional Split
    "5B851C52-5F4B-4CB9-82C8-CABB5D36D6EF": "transform",   # Data Conversion
    "2AD76583-6C4C-433B-B9AB-8CE75E74FCA8": "transform",   # Row Count
    "9B14D6D8-18CF-4CE9-B48C-B6B4CE50C48A": "transform",   # Merge Join
    "2AC501E3-4C81-460C-9BC9-91D8A44C6F1E": "transform",   # Union All
    "5B2F5695-63F7-4D4C-9BFD-E98F80B62D1C": "transform",   # Multicast
    "EC139065-4B65-43B9-A3BE-7B235F421E5B": "transform",   # Script Component
    "E5D31A7D-CD15-4D5D-9B77-9E9B5CF3B4AA": "transform",   # Pivot
    "D3B89F4E-5B4A-4D5F-9D4D-9B77A4B3B8E5": "transform",   # Unpivot
}


def _classify_component(comp: SSISDataFlowComponent) -> str:
    """Return 'source', 'destination', or 'transform'."""
    cls = comp.component_class

    # 1. GUID lookup (old-format class IDs like {2C0A8BE5-...})
    guid_key = cls.strip("{}").upper()
    if guid_key in _GUID_CATEGORY:
        return _GUID_CATEGORY[guid_key]

    cls_lower = cls.lower()

    # 2. Class-name substring match
    for s in _SOURCE_CLASSES:
        if s in cls_lower:
            return "source"
    for d in _DEST_CLASSES:
        if d in cls_lower:
            return "destination"

    # 3. Fall back to component *name* heuristics (works for old-format GUID class IDs)
    name_lower = comp.name.lower()
    if any(kw in name_lower for kw in ("source", "reader", "input", "extract")):
        return "source"
    if any(kw in name_lower for kw in ("destination", "dest", "output", "writer", "load")):
        return "destination"

    return "transform"


def _m_string(val: Optional[str]) -> str:
    """Escape a value for use in a Power Query M string literal."""
    if not val:
        return '""'
    return '"' + val.replace('"', '""') + '"'


def _conn_expr(cm: Any) -> str:
    """
    Return a Power Query M expression that opens a connection.
    cm is an SSISConnectionManager (or None).
    """
    if cm is None:
        return 'Sql.Database("TODO_SERVER", "TODO_DATABASE")  // TODO: set real server/database'
    ct = (cm.connection_type or "").upper()
    server = cm.server_name or "TODO_SERVER"
    database = cm.database_name or "TODO_DATABASE"
    url = cm.url or cm.connection_string or "TODO_URL"
    file_path = cm.file_path or cm.connection_string or "TODO_PATH"

    if "HTTP" in ct or "WEB" in ct:
        return f'Web.Contents({_m_string(url)})'
    if "FLAT" in ct or "FILE" in ct:
        return f'File.Contents({_m_string(file_path)})'
    if "EXCEL" in ct:
        # Excel via file path — use the connection string (full path is in server_name for EXCEL)
        path = cm.connection_string or server
        return f'Excel.Workbook(File.Contents({_m_string(path)}), null, true)  // TODO: migrate to Fabric Lakehouse'
    # Default: SQL / OLEDB / ADO.NET
    return f'Sql.Database({_m_string(server)}, {_m_string(database)})'


def _source_to_m(
    comp: SSISDataFlowComponent,
    step_name: str,
    cm: Any = None,   # SSISConnectionManager | None
) -> str:
    """Generate M code for a source component."""
    cls = comp.component_class.lower()
    table = comp.table_name or comp.properties.get("OpenRowset") or "TODO_TABLE"
    sql = comp.sql_command or comp.properties.get("SqlCommand") or ""

    # Sanitize step name for M
    safe_name = re.sub(r"[^A-Za-z0-9_]", "_", step_name)

    # Build the connection expression from the linked connection manager
    conn_expr = _conn_expr(cm)

    if "flatfile" in cls:
        path = (cm.connection_string if cm else None) or comp.properties.get("ConnectionString") or "TODO_PATH"
        return (
            f"    {safe_name} = \n"
            f"        // TODO: Replace with actual Fabric lakehouse or cloud path\n"
            f"        Csv.Document(File.Contents({_m_string(path)}), [Delimiter=\",\", "
            f"Columns=null, Encoding=1252, QuoteStyle=QuoteStyle.None])"
        )

    if sql:
        return (
            f"    {safe_name} =\n"
            f"        Value.NativeQuery(\n"
            f"            {conn_expr},\n"
            f"            {_m_string(sql)}, null,\n"
            f"            [EnableFolding=true]\n"
            f"        )"
        )

    return (
        f"    {safe_name} =\n"
        f"        Value.NativeQuery({conn_expr}, \"SELECT * FROM {table}\", null)"
    )


def _transform_to_m(comp: SSISDataFlowComponent, step_name: str, prev_step: str) -> str:
    """Generate M code for a transformation component."""
    cls = comp.component_class.lower()
    safe_name = re.sub(r"[^A-Za-z0-9_]", "_", step_name)
    safe_prev = re.sub(r"[^A-Za-z0-9_]", "_", prev_step) if prev_step else "Source"

    transform_type = "Unknown"
    for key, ttype in _TRANSFORM_CLASSES.items():
        if key in cls:
            transform_type = ttype
            break
    # GUID class IDs won't match above — fall back to component name heuristic
    if transform_type == "Unknown":
        name_lower = comp.name.lower()
        for key, ttype in _TRANSFORM_CLASSES.items():
            if key in name_lower:
                transform_type = ttype
                break

    if transform_type == "DerivedColumn":
        lines = ["        // TODO: Add derived columns based on SSIS expressions"]
        for col in comp.output_columns:
            col_name = col.get("name", "NewCol")
            lines.append(
                f"        // {col_name} = <expression>"
            )
        body = "\n".join(lines)
        return (
            f"    {safe_name} =\n"
            f"        // Derived Column transform - review expressions\n"
            f"        Table.AddColumn({safe_prev}, \"TODO_DerivedCol\",\n"
            f"            each null, type text) // TODO: Replace with actual logic\n"
        )

    if transform_type == "Aggregate":
        return (
            f"    {safe_name} =\n"
            f"        // Aggregate transform\n"
            f"        // TODO: replace with Table.Group(...) with correct key columns and aggregations\n"
            f"        Table.Group({safe_prev}, {{\"TODO_KEY\"}}, {{{{\"TODO_AGG\", each List.Sum([TODO_COL]), type number}}}})"
        )

    if transform_type == "Sort":
        return (
            f"    {safe_name} =\n"
            f"        // Sort transform\n"
            f"        Table.Sort({safe_prev}, {{{{\"TODO_COL\", Order.Ascending}}}}) // TODO: update columns"
        )

    if transform_type == "MergeJoin":
        return (
            f"    {safe_name} =\n"
            f"        // MergeJoin transform\n"
            f"        // TODO: supply right-hand table and join keys\n"
            f"        Table.NestedJoin({safe_prev}, {{\"TODO_LEFT_KEY\"}}, TODO_RIGHT, {{\"TODO_RIGHT_KEY\"}}, \"_merged\", JoinKind.Inner)"
        )

    if transform_type == "Lookup":
        return (
            f"    {safe_name} =\n"
            f"        // Lookup transform\n"
            f"        // TODO: configure lookup table and key columns\n"
            f"        Table.NestedJoin({safe_prev}, {{\"TODO_KEY\"}}, TODO_LOOKUP_TABLE, {{\"TODO_LOOKUP_KEY\"}}, \"_lookup\", JoinKind.LeftOuter)"
        )

    if transform_type == "UnionAll":
        return (
            f"    {safe_name} =\n"
            f"        // Union All transform\n"
            f"        // TODO: list all input tables\n"
            f"        Table.Combine({{{safe_prev}, TODO_TABLE2}})"
        )

    if transform_type == "DataConversion":
        return (
            f"    {safe_name} =\n"
            f"        // Data Conversion transform\n"
            f"        // TODO: add type casts for each column\n"
            f"        Table.TransformColumnTypes({safe_prev}, {{{{\"TODO_COL\", type text}}}})"
        )

    if transform_type == "Pivot":
        return (
            f"    {safe_name} =\n"
            f"        // Pivot transform\n"
            f"        // TODO: configure pivot column and value column\n"
            f"        Table.Pivot({safe_prev}, List.Distinct({safe_prev}[TODO_PIVOT_COL]), \"TODO_PIVOT_COL\", \"TODO_VALUE_COL\")"
        )

    if transform_type == "Unpivot":
        return (
            f"    {safe_name} =\n"
            f"        // Unpivot transform\n"
            f"        // TODO: configure columns to unpivot\n"
            f"        Table.UnpivotOtherColumns({safe_prev}, {{\"TODO_ID_COL\"}}, \"Attribute\", \"Value\")"
        )

    if transform_type == "ConditionalSplit":
        return (
            f"    {safe_name} =\n"
            f"        // ConditionalSplit transform - produces a filtered table\n"
            f"        // TODO: replicate each split condition as a separate query step or output table\n"
            f"        Table.SelectRows({safe_prev}, each true) // Replace 'true' with actual condition"
        )

    # Generic fallback
    return (
        f"    {safe_name} =\n"
        f"        // {transform_type} transform - manual conversion required\n"
        f"        {safe_prev} // TODO: implement {transform_type} logic"
    )


def _destination_to_m(comp: SSISDataFlowComponent, step_name: str, prev_step: str) -> str:
    """Generate a comment step for a destination (data loading is handled by Fabric pipelines)."""
    safe_name = re.sub(r"[^A-Za-z0-9_]", "_", step_name)
    safe_prev = re.sub(r"[^A-Za-z0-9_]", "_", prev_step) if prev_step else "Source"
    table = comp.table_name or comp.properties.get("OpenRowset") or "TODO_DESTINATION_TABLE"
    return (
        f"    {safe_name} =\n"
        f"        // Destination: {comp.component_class}\n"
        f"        // Table/file: {table}\n"
        f"        // TODO: In Fabric, data is written using a Copy activity or Lakehouse table output.\n"
        f"        // Returning the transformed data as the query output here.\n"
        f"        {safe_prev}"
    )


def dataflow_to_m(df: SSISDataFlow, conn_map: Optional[Dict[str, Any]] = None) -> str:
    """
    Convert an SSISDataFlow to a Power Query M document string.
    conn_map: dict keyed by connection id or name → SSISConnectionManager
    """
    if not df.components:
        return (
            f"section Section1; shared {re.sub(r'[^A-Za-z0-9_]', '_', df.name)} = "
            f"// TODO: No components found in data flow '{df.name}'\n"
            f"    #table(type table [Column1=text], {{{{\"TODO\"}}}});"
        )

    conn_map = conn_map or {}

    # Walk components in a rough topological order using paths
    # Build adjacency: comp_id → comp_id (via paths)
    id_to_comp: Dict[str, SSISDataFlowComponent] = {c.id: c for c in df.components}

    # Determine order: sources first, then transforms, then destinations
    sources = [c for c in df.components if _classify_component(c) == "source"]
    transforms = [c for c in df.components if _classify_component(c) == "transform"]
    destinations = [c for c in df.components if _classify_component(c) == "destination"]
    ordered = sources + transforms + destinations

    steps: List[str] = []
    prev_step = None

    for i, comp in enumerate(ordered):
        step_name = re.sub(r"[^A-Za-z0-9_]", "_", comp.name) or f"Step_{i}"
        category = _classify_component(comp)
        cm = conn_map.get(comp.connection_ref or "") if comp.connection_ref else None

        if category == "source":
            steps.append(_source_to_m(comp, step_name, cm))
        elif category == "transform":
            prev = re.sub(r"[^A-Za-z0-9_]", "_", ordered[i - 1].name) if i > 0 else "Source"
            steps.append(_transform_to_m(comp, step_name, prev))
        else:
            prev = re.sub(r"[^A-Za-z0-9_]", "_", ordered[i - 1].name) if i > 0 else "Source"
            steps.append(_destination_to_m(comp, step_name, prev))

        prev_step = step_name

    # The output of the query is the last step
    last_step = re.sub(r"[^A-Za-z0-9_]", "_", ordered[-1].name) if ordered else "Source"
    query_name = re.sub(r"[^A-Za-z0-9_]", "_", df.name) or "DataFlow"

    steps_body = ",\n".join(steps)
    m_doc = (
        f"section Section1;\n\n"
        f"shared {query_name} = let\n"
        f"{steps_body}\n"
        f"in\n"
        f"    {last_step};"
    )
    return m_doc


def build_dataflow_definition(df: SSISDataFlow, conn_map: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Build the Fabric Dataflow Gen2 item definition payload.

    Parts required by the Dataflow Gen2 definition API:
      - queryMetadata.json  (metadata about queries)
      - mashup.pq           (Power Query M document with StagingDefinition prefix)
    """
    m_body = dataflow_to_m(df, conn_map=conn_map)
    query_name = re.sub(r"[^A-Za-z0-9_]", "_", df.name) or "DataFlow"

    # mashup.pq requires the StagingDefinition annotation prefix
    mashup_pq = f"[StagingDefinition = [Kind = \"FastCopy\"]]\n{m_body}"
    mashup_encoded = base64.b64encode(mashup_pq.encode("utf-8")).decode("ascii")

    # queryMetadata.json — describes each shared query in the M document
    query_id = str(uuid.uuid4())
    metadata = {
        "formatVersion": "202502",
        "computeEngineSettings": {"allowFastCopy": True},
        "name": df.name,
        "queryGroups": [],
        "documentLocale": "en-US",
        "queriesMetadata": {
            query_name: {
                "queryId": query_id,
                "queryName": query_name,
                "queryGroupId": None,
                "isHidden": False,
                "loadEnabled": True,
            }
        },
        "allowNativeQueries": True,
    }
    metadata_encoded = base64.b64encode(
        json.dumps(metadata, indent=2).encode("utf-8")
    ).decode("ascii")

    return {
        "definition": {
            "parts": [
                {
                    "path": "queryMetadata.json",
                    "payload": metadata_encoded,
                    "payloadType": "InlineBase64",
                },
                {
                    "path": "mashup.pq",
                    "payload": mashup_encoded,
                    "payloadType": "InlineBase64",
                },
            ]
        }
    }


def convert_dataflows(
    data_flows: List[SSISDataFlow],
    conn_map: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    """Return a list of Fabric Dataflow Gen2 creation descriptors."""
    results = []
    for df in data_flows:
        results.append({
            "ssis_name": df.name,
            "ssis_id": df.id,
            "display_name": df.name,
            "definition": build_dataflow_definition(df, conn_map=conn_map),
        })
    return results
