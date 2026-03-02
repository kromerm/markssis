# ssis2fabric

**Convert SSIS DTSX packages to Microsoft Fabric Data Factory items.**

`ssis2fabric` is a Python command-line tool that reads a SQL Server Integration Services (SSIS) `.dtsx` package file and creates the following Fabric items in a target workspace:

| SSIS artifact | Fabric artifact |
|---|---|
| Package control flow | **Data Pipeline** |
| Data Flow task | **Dataflow Gen2** (Power Query M) |
| Connection Manager | **Fabric Shareable Connection** |

Activities that cannot be fully auto-converted are created with **`state: InActive`** so the pipeline can still be saved and opened in Fabric — they just need follow-up manual editing.

---

## Prerequisites

- Python 3.9+
- A Microsoft Fabric workspace where you have **Contributor** or higher access
- A browser (for interactive Microsoft Entra ID sign-in)

---

## Installation

### Option A — Install the latest release wheel (recommended)

Download the `.whl` from the [Releases page](https://github.com/kromerm/markssis/releases/latest), then:

```bash
pip install ssis2fabric-0.1.0-py3-none-any.whl
```

This installs the `ssis2fabric` command directly onto your PATH and pulls in all dependencies automatically.

### Option B — Install from source

```bash
git clone https://github.com/kromerm/markssis.git
cd markssis
pip install .
```

### Option C — Editable / development install

```bash
git clone https://github.com/kromerm/markssis.git
cd markssis
pip install -e .
```

After any of the above you can verify the install with:

```bash
ssis2fabric --help
```

---

## Quick Start

### 1. Dry run — review the conversion without touching Fabric

Parse your package, convert it, and write the JSON output locally. No Fabric API calls are made and no authentication is required.

```bash
ssis2fabric \
  --dtsx  MyPackage.dtsx \
  --workspace-id  xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx \
  --dry-run \
  --output-dir  output/
```

Expected output:
```
============================================================
  Parsing DTSX: MyPackage.dtsx
============================================================

============================================================
  SSIS Package Summary
============================================================
  Package name   : MyPackage
  Variables      : 3
  Connections    : 2
  Top-level tasks: 5
  Data flows     : 1
  Precedence     : 4

  Connection Managers:
    - DW  [OLEDB:SQL]  server=myserver  db=DW
    - FlatFiles  [FILE]

  Control Flow Tasks:
    - Load Staging  [DataFlow]
    - Send Status Mail  [SendMail]
    - Cleanup  [ExecuteSQL]
    - Archive Files  [FileSystem]
    - Notify  [SendMail]

============================================================
  Dry Run – building pipeline definition only
============================================================
  [saved] output/connections.json
  [saved] output/dataflow_Load Staging.json
  [saved] output/pipeline_MyPackage.json

  [dry-run] No Fabric API calls made.
```

### 2. Full migration — create items in Fabric

A browser window will open for Microsoft Entra ID sign-in on first use.

```bash
ssis2fabric \
  --dtsx  MyPackage.dtsx \
  --workspace-id  xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx \
  --folder  "SSIS Migration"
```

### 3. Override the pipeline display name

```bash
ssis2fabric \
  --dtsx  MyPackage.dtsx \
  --workspace-id  xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx \
  --folder  "SSIS Migration" \
  --pipeline-name  "Daily Sales Load"
```

### 4. Skip dataflow creation (pipeline only)

Useful if you only need the control-flow pipeline and want to wire up dataflows manually.

```bash
ssis2fabric \
  --dtsx  MyPackage.dtsx \
  --workspace-id  xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx \
  --no-dataflows
```

### 5. Skip connection creation (use dummy connection IDs)

Useful when connections already exist in the workspace and you only want the pipeline/dataflow items.

```bash
ssis2fabric \
  --dtsx  MyPackage.dtsx \
  --workspace-id  xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx \
  --no-connections
```

### 6. Save artifacts locally AND create in Fabric

```bash
ssis2fabric \
  --dtsx  MyPackage.dtsx \
  --workspace-id  xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx \
  --folder  "SSIS Migration" \
  --output-dir  output/
```

### 7. Verbose mode — debug HTTP calls

```bash
ssis2fabric \
  --dtsx  MyPackage.dtsx \
  --workspace-id  xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx \
  --dry-run \
  --verbose
```

---

## Options

| Flag | Description |
|---|---|
| `--dtsx PATH` | **Required.** Path to the `.dtsx` file |
| `--workspace-id GUID` | **Required.** Target Fabric workspace GUID |
| `--folder NAME` | Optional folder name to place items in |
| `--pipeline-name NAME` | Override the Fabric pipeline display name (default: SSIS package name) |
| `--dry-run` | Parse + convert but do **not** call Fabric APIs |
| `--verbose` / `-v` | Print HTTP request/response details |
| `--output-dir DIR` | Save converted JSON artifacts to this directory |
| `--no-connections` | Skip Fabric connection creation |
| `--no-dataflows` | Skip Fabric Dataflow Gen2 creation |

---

## Authentication

The tool uses **Microsoft Entra ID interactive browser authentication** (`InteractiveBrowserCredential` from `azure-identity`).  On first run a browser window will open to https://login.microsoftonline.com — sign in with the account that has access to the target Fabric workspace.

The token is cached in memory for the duration of the run.

---

## SSIS → Fabric Mapping Details

### Control Flow → Pipeline Activities

| SSIS Task | Fabric Activity | Notes |
|---|---|---|
| Execute SQL Task | `Script` or `SqlServerStoredProcedure` | SP name detected automatically |
| Data Flow Task | `RefreshDataFlow` | References the Dataflow Gen2 created for that task |
| ForEach Loop | `ForEach` | Inner tasks recursively converted |
| For Loop Container | `Until` | ⚠ Set InActive – loop expressions converted to TODO placeholder (SSIS expression syntax differs from Fabric) |
| Sequence Container | `IfCondition` (always-true wrapper) | Inner tasks recursively converted |
| Execute Package Task | `ExecutePipeline` | ⚠ Set InActive – referenced pipeline may not exist yet |
| Script Task | `Script` | ⚠ Set InActive – logic must be manually ported |
| Send Mail Task | `Office365Email` | ⚠ Set InActive – connection required; fields From/To/CC/BCC/Subject/Body/Priority/Attachments populated |
| Web Service Task | `WebActivity` (HTTP POST) | ⚠ Set InActive – SOAP endpoint URL, headers, and envelope body must be updated |
| FTP Task (Receive) | `Copy` (FTP → ADLS/lakehouse) | ⚠ Set InActive – linked service and dataset configuration required |
| FTP Task (Send) | `Copy` (ADLS/lakehouse → FTP) | ⚠ Set InActive – linked service and dataset configuration required |
| FTP Task (DeleteRemoteFile / DeleteLocalFile) | `Delete` | ⚠ Set InActive – linked service configuration required |
| FTP Task (directory ops / rename) | `Script` | ⚠ Set InActive – no direct Fabric equivalent |
| Execute Process Task | `WebActivity` | ⚠ Set InActive – no direct equivalent |
| File System Task | `Script` | ⚠ Set InActive – rework using Lakehouse file APIs |
| Bulk Insert Task | `Copy` | ⚠ Set InActive – source/sink configuration required |
| All others | `Wait` (1 s) | ⚠ Set InActive – manual replacement required |

> **Disabled tasks:** Any SSIS task with `DTS:Disabled="True"` is emitted with `state: InActive` and its description prefixed with `[Disabled in original SSIS package]`.

> **SSIS Variables → Pipeline Parameters:** `User::` namespace variables are converted to Fabric pipeline parameters (type mapping: String/DateTime → `string`, Int32/Int64 → `int`, Double/Decimal → `float`, Boolean → `bool`). `System::` variables are skipped.

### Data Flow → Dataflow Gen2 (Power Query M)

Components are mapped to M expression steps:

| SSIS Component | M Expression |
|---|---|
| OLE DB / ADO Source | `Value.NativeQuery(Source, sql)` |
| Flat File Source | `Csv.Document(File.Contents(...))` |
| Derived Column | `Table.AddColumn` |
| Aggregate | `Table.Group` |
| Sort | `Table.Sort` |
| Merge Join | `Table.NestedJoin` |
| Lookup | `Table.NestedJoin` (left outer) |
| Union All | `Table.Combine` |
| Data Conversion | `Table.TransformColumnTypes` |
| Pivot | `Table.Pivot` |
| Unpivot | `Table.UnpivotOtherColumns` |
| Conditional Split | `Table.SelectRows` |
| Destination | Comment only – data loading done via Copy activity |

All steps contain `// TODO` annotations where manual adjustment is needed.

### Connections → Fabric Connections

SSIS connection types are mapped to Fabric connectivity types:

| SSIS Type | Fabric Type |
|---|---|
| OLEDB / ADO.NET | SQL (ShareableCloud) |
| File / Flat File | File |
| HTTP | Web (Anonymous) |
| FTP | FTP |
| SMTP / others | SQL (dummy) |

Connections are created with **`skipTestConnection: true`** and dummy/placeholder credentials so the API call succeeds.  **Credentials must be updated in Fabric after migration.**

---

## Output Files

When `--output-dir` is specified the following files are written:

| File | Contents |
|---|---|
| `connections.json` | Array of Fabric connection creation payloads |
| `dataflow_<name>.json` | Fabric Dataflow Gen2 item definition |
| `pipeline_<name>.json` | Fabric pipeline-content.json (base64-wrapped) |

---

## Post-Migration Checklist

1. Open the pipeline in Fabric and find activities shown with a ⚠ badge (InActive).
2. For each InActive activity:
   - Review the `description` field for original SSIS task details.
   - Update connection references, SQL statements, or script logic.
   - Set `state` back to `Active` once the activity is ready.
3. Open each Dataflow Gen2 item and:
   - Set real data source connections.
   - Fix `// TODO` expressions throughout the M query.
4. Update connection credentials via **Fabric > Manage connections and gateways**.
5. Run the pipeline in Debug mode and iterate.

---

## Project Structure

```
SSIS3/
├── ssis2fabric/
│   ├── __init__.py
│   ├── __main__.py        # python -m ssis2fabric
│   ├── cli.py             # CLI argument parsing & orchestration
│   ├── models.py          # SSIS dataclass models
│   ├── parser.py          # DTSX XML parser
│   ├── converters/
│   │   ├── __init__.py
│   │   ├── connections.py # Connection Manager → Fabric Connection
│   │   ├── dataflow.py    # Data Flow → Dataflow Gen2 (M query)
│   │   └── pipeline.py    # Control Flow → Fabric pipeline
│   └── fabric/
│       ├── __init__.py
│       └── client.py      # Fabric REST API client (user-auth)
├── requirements.txt
├── pyproject.toml
└── README.md
```
