"""
ssis2fabric CLI – Convert a SSIS DTSX package to Microsoft Fabric Data Factory items.

Usage
-----
  python -m ssis2fabric.cli --dtsx <path> --workspace-id <guid> [options]

  --dtsx          Path to the .dtsx file (required)
  --workspace-id  Target Fabric workspace GUID (required)
  --folder        Folder name inside the workspace (optional)
  --pipeline-name Override the Fabric pipeline display name (default: SSIS package name)
  --dry-run       Parse + convert but do NOT call Fabric APIs
  --verbose       Print detailed HTTP request/response info
  --output-dir    Write converted JSON files to this directory (optional)
  --no-connections  Skip creating Fabric connections
  --no-dataflows    Skip creating Fabric dataflows
"""
import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

# Ensure UTF-8 output on Windows consoles that default to cp1252
if sys.stdout.encoding and sys.stdout.encoding.lower() not in ("utf-8", "utf8"):
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

from .parser import parse_dtsx
from .models import SSISPackage, SSISDataFlow
from .converters.connections import convert_connections
from .converters.dataflow import convert_dataflows, build_dataflow_definition
from .converters.pipeline import build_pipeline_definition


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _print_section(title: str) -> None:
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}")


def _save_json(output_dir: Optional[str], filename: str, data: Any) -> None:
    if not output_dir:
        return
    os.makedirs(output_dir, exist_ok=True)
    path = os.path.join(output_dir, filename)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(data, fh, indent=2, ensure_ascii=False)
    print(f"  [saved] {path}")


# ---------------------------------------------------------------------------
# Conversion summary
# ---------------------------------------------------------------------------

def _print_summary(package: SSISPackage) -> None:
    _print_section("SSIS Package Summary")
    print(f"  Package name   : {package.name}")
    print(f"  Package ID     : {package.id}")
    print(f"  Description    : {package.description or '(none)'}")
    print(f"  Variables      : {len(package.variables)}")
    print(f"  Connections    : {len(package.connection_managers)}")
    print(f"  Top-level tasks: {len(package.tasks)}")
    print(f"  Data flows     : {len(package.data_flows)}")
    print(f"  Precedence     : {len(package.precedence_constraints)}")

    if package.connection_managers:
        print(f"\n  Connection Managers:")
        for cm in package.connection_managers:
            print(f"    - {cm.name} [{cm.connection_type}]  server={cm.server_name or '?'}  db={cm.database_name or '?'}")

    if package.data_flows:
        print(f"\n  Data Flows:")
        for df in package.data_flows:
            print(f"    - {df.name}  ({len(df.components)} components, {len(df.paths)} paths)")

    if package.tasks:
        print(f"\n  Control Flow Tasks:")
        from .models import SSISForEachLoop, SSISForLoop, SSISSequenceContainer
        for t in package.tasks:
            task_type = getattr(t, "task_type", type(t).__name__)
            print(f"    - {t.name}  [{task_type}]")


# ---------------------------------------------------------------------------
# Main orchestration
# ---------------------------------------------------------------------------

def run(
    dtsx_path: str,
    workspace_id: str,
    folder_name: Optional[str],
    pipeline_name: Optional[str],
    dry_run: bool,
    verbose: bool,
    output_dir: Optional[str],
    create_connections: bool,
    create_dataflows_flag: bool,
) -> None:
    # ------------------------------------------------------------------ Parse
    _print_section(f"Parsing DTSX: {dtsx_path}")
    try:
        package = parse_dtsx(dtsx_path)
    except Exception as exc:
        print(f"[ERROR] Failed to parse DTSX file: {exc}", file=sys.stderr)
        sys.exit(1)

    # Determine the pipeline display name: CLI override takes precedence
    pipeline_display_name = (pipeline_name or package.name).strip()

    _print_summary(package)

    # -------------------------------------------- Convert connections payload
    _print_section("Converting Connection Managers")
    conn_descriptors = convert_connections(package.connection_managers)
    for desc in conn_descriptors:
        print(f"  -> {desc['ssis_name']}  ({desc['payload']['connectionDetails']['type']})")
    _save_json(output_dir, "connections.json", conn_descriptors)

    # ---------------------------------------------- Convert dataflow payloads
    _print_section("Converting Data Flows → Dataflow Gen2")
    # Build a lookup map so the M-query generator can resolve connection managers
    conn_map: dict = {}
    for cm in package.connection_managers:
        if cm.id:
            conn_map[cm.id] = cm
        if cm.name:
            conn_map[cm.name] = cm
    df_descriptors = convert_dataflows(package.data_flows, conn_map=conn_map)
    for desc in df_descriptors:
        print(f"  -> {desc['display_name']}  ({len(package.data_flows[0].components) if package.data_flows else 0} components)")
        _save_json(output_dir, f"dataflow_{desc['display_name']}.json", desc["definition"])

    # ----------------------------------------------------------------- Auth + Fabric client
    if dry_run:
        _print_section("Dry Run – building pipeline definition only")
        # Build placeholder maps for output
        conn_id_map: Dict[str, str] = {
            cm.id: "DRY-RUN-CONN-ID" for cm in package.connection_managers
        }
        conn_id_map.update({cm.name: "DRY-RUN-CONN-ID" for cm in package.connection_managers})
        df_id_map: Dict[str, str] = {df.id: "DRY-RUN-DF-ID" for df in package.data_flows}
        pipeline_def = build_pipeline_definition(package, conn_id_map, df_id_map, workspace_id)
        _save_json(output_dir, f"pipeline_{package.name}.json", pipeline_def)
        print("")
        print("  [dry-run] No Fabric API calls made.")
        if output_dir:
            print(f"  [dry-run] Converted JSON written to: {output_dir}")
        return

    # ---------------------------------------------------------------- Fabric client
    print("")
    print("  Authenticating with Microsoft Fabric (interactive browser login)…")
    try:
        from .fabric.client import FabricClient, FabricAPIError
    except ImportError as e:
        print(f"[ERROR] Could not import Fabric client: {e}", file=sys.stderr)
        sys.exit(1)

    client = FabricClient(verbose=verbose)

    # Test auth early
    try:
        _print_section("Resolving target folder")
        folder_id = client.get_or_create_folder(workspace_id, folder_name or "")
        if folder_name:
            print(f"  Folder '{folder_name}' → id={folder_id or '(root)'}")
        else:
            print("  No folder specified – items created at workspace root.")
    except Exception as exc:
        print(f"[ERROR] Authentication or workspace access failed: {exc}", file=sys.stderr)
        sys.exit(1)

    # -------------------------------------------------------- Create connections
    conn_id_map: Dict[str, str] = {}

    if create_connections and conn_descriptors:
        _print_section("Creating Fabric Connections")
        for desc in conn_descriptors:
            name = desc["ssis_name"]
            print(f"  Creating connection: {name} …", end="", flush=True)
            try:
                result = client.create_connection(desc["payload"])
                conn_guid = result.get("id") or result.get("connectionId") or "UNKNOWN"
                print(f" OK  id={conn_guid}")
                # Map both id and name
                conn_id_map[desc["ssis_id"]] = conn_guid
                conn_id_map[name] = conn_guid
            except Exception as exc:
                print(f" FAIL  {exc}")
                # Use dummy id so pipeline creation can still proceed
                conn_id_map[desc["ssis_id"]] = "00000000-0000-0000-0000-000000000000"
                conn_id_map[name] = "00000000-0000-0000-0000-000000000000"
                print(f"  [warn] Using dummy connection ID for '{name}'")
    else:
        # Fill with dummies
        for cm in package.connection_managers:
            conn_id_map[cm.id] = "00000000-0000-0000-0000-000000000000"
            conn_id_map[cm.name] = "00000000-0000-0000-0000-000000000000"

    # -------------------------------------------------------- Create dataflows
    df_id_map: Dict[str, str] = {}

    if create_dataflows_flag and df_descriptors:
        _print_section("Creating Fabric Dataflow Gen2 items")
        for desc in df_descriptors:
            name = desc["display_name"]
            print(f"  Creating dataflow: {name} …", end="", flush=True)
            try:
                result = client.create_dataflow(
                    workspace_id=workspace_id,
                    display_name=name,
                    definition=desc["definition"],
                    description=f"Converted from SSIS Data Flow '{name}'",
                    folder_id=folder_id,
                )
                df_guid = result.get("id") or "UNKNOWN"
                print(f" ✓  id={df_guid}")
                df_id_map[desc["ssis_id"]] = df_guid
            except Exception as exc:
                print(f" ✗  {exc}")
                df_id_map[desc["ssis_id"]] = "00000000-0000-0000-0000-000000000000"
                print(f"  [warn] Using dummy dataflow ID for '{name}'")
    else:
        for df in package.data_flows:
            df_id_map[df.id] = "00000000-0000-0000-0000-000000000000"

    # -------------------------------------------------------- Build + create pipeline
    _print_section(f"Creating Fabric Data Pipeline: {pipeline_display_name}")
    pipeline_def = build_pipeline_definition(package, conn_id_map, df_id_map, workspace_id)
    _save_json(output_dir, f"pipeline_{pipeline_display_name}.json", pipeline_def)

    print(f"  Creating pipeline: {pipeline_display_name} …", end="", flush=True)
    try:
        result = client.create_pipeline(
            workspace_id=workspace_id,
            display_name=pipeline_display_name,
            definition=pipeline_def,
            description=(
                f"Converted from SSIS package '{package.name}'. "
                "InActive activities require manual follow-up."
            ),
            folder_id=folder_id,
        )
        pipeline_guid = result.get("id") or "UNKNOWN"
        print(f" ✓  id={pipeline_guid}")
    except Exception as exc:
        print(f" ✗  {exc}")
        print(
            "\n[WARN] Pipeline creation failed.  The converted pipeline-content.json "
            "has been written to the output directory (if --output-dir was specified). "
            "You can import it manually in Fabric."
        )
        sys.exit(1)

    _print_section("Migration Complete")
    print(f"  Pipeline '{pipeline_display_name}'   → {pipeline_guid}")
    print(f"  Connections created        : {len([v for v in conn_id_map.values() if v != '00000000-0000-0000-0000-000000000000'])}")
    print(f"  Dataflows created          : {len([v for v in df_id_map.values() if v != '00000000-0000-0000-0000-000000000000'])}")
    print("")
    print("  Next steps:")
    print("  1. Open the pipeline in Fabric and review activities marked [InActive].")
    print("  2. Update TODO placeholder connection IDs and stored procedure names.")
    print("  3. Review Dataflow Gen2 M queries and fix TODO annotations.")
    print("  4. Update connection credentials for any dummy connections.")
    print("")


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        prog="ssis2fabric",
        description="Convert an SSIS DTSX package to Microsoft Fabric Data Factory items.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--dtsx", required=True,
        help="Path to the .dtsx package file.",
    )
    parser.add_argument(
        "--workspace-id", required=True, dest="workspace_id",
        help="Target Fabric workspace GUID.",
    )
    parser.add_argument(
        "--folder", default=None, dest="folder",
        help="Optional folder name inside the workspace to place items.",
    )
    parser.add_argument(
        "--pipeline-name", default=None, dest="pipeline_name",
        help="Override the Fabric pipeline display name (default: SSIS package name).",
    )
    parser.add_argument(
        "--dry-run", action="store_true", dest="dry_run",
        help="Parse and convert but do NOT call Fabric APIs.",
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true",
        help="Print detailed HTTP request/response information.",
    )
    parser.add_argument(
        "--output-dir", default=None, dest="output_dir",
        help="Optional directory to save converted JSON artifacts.",
    )
    parser.add_argument(
        "--no-connections", action="store_true", dest="no_connections",
        help="Skip creating Fabric connections (use dummy IDs in pipeline).",
    )
    parser.add_argument(
        "--no-dataflows", action="store_true", dest="no_dataflows",
        help="Skip creating Fabric Dataflow Gen2 items.",
    )

    args = parser.parse_args()

    dtsx_path = os.path.abspath(args.dtsx)
    if not os.path.isfile(dtsx_path):
        print(f"[ERROR] DTSX file not found: {dtsx_path}", file=sys.stderr)
        sys.exit(1)

    run(
        dtsx_path=dtsx_path,
        workspace_id=args.workspace_id,
        folder_name=args.folder,
        pipeline_name=args.pipeline_name,
        dry_run=args.dry_run,
        verbose=args.verbose,
        output_dir=args.output_dir,
        create_connections=not args.no_connections,
        create_dataflows_flag=not args.no_dataflows,
    )


if __name__ == "__main__":
    main()
