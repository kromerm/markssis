# ssis2fabric — Product Requirements Document

**Version:** 1.0  
**Date:** March 2026  
**Status:** Implemented (v0.1.0)  
**Repository:** https://github.com/kromerm/markssis

---

## 1. Overview

### 1.1 Purpose

`ssis2fabric` is a Python command-line tool that automates the migration of SQL Server Integration Services (SSIS) packages (`.dtsx` files) to Microsoft Fabric Data Factory items. The tool reduces the manual effort required when moving legacy SSIS workloads to Microsoft Fabric by parsing SSIS package XML and generating equivalent Fabric artifacts via the Fabric REST API.

### 1.2 Goals

- Eliminate manual re-authoring of simple SSIS control-flow packages in Fabric.
- Produce fully valid Fabric pipeline, dataflow, and connection JSON payloads.
- Handle tasks that have no direct Fabric equivalent gracefully — create them as `InActive` placeholders so the pipeline can still be opened and fixed up in the Fabric UI.
- Be runnable in a `--dry-run` mode so engineers can review converted output before touching a live workspace.
- Be distributable as a single `.whl` file installable via `pip`.

### 1.3 Non-Goals (v0.1.0)

- Full round-trip fidelity for all SSIS task types (complex Script Tasks, custom components, etc. are left as InActive placeholders).
- Automated credential migration (credentials must be set in Fabric after migration).
- Support for SSIS packages using the legacy Package Deployment Model (project-level connections, project parameters).
- GUI or web interface.

---

## 2. Users and Personas

| Persona | Description |
|---|---|
| **Data Engineer / Architect** | Running the tool against production SSIS packages to bootstrap a Fabric migration. Values accuracy and completeness of the conversion. |
| **Solution Architect** | Using `--dry-run` + `--output-dir` to audit what will be created before committing to a workspace. Values reviewability. |
| **Migration Project Manager** | Interested in the post-migration checklist and understanding which items need manual follow-up. |

---

## 3. Functional Requirements

### 3.1 CLI Interface

| ID | Requirement |
|---|---|
| CLI-01 | The tool MUST accept a path to a `.dtsx` file via `--dtsx`. |
| CLI-02 | The tool MUST accept a target Fabric workspace GUID via `--workspace-id`. |
| CLI-03 | The tool MUST support an optional `--folder` argument to place created items inside a named folder in the workspace. |
| CLI-04 | The tool MUST support an optional `--pipeline-name` argument that overrides the Fabric pipeline display name (default: SSIS package name). |
| CLI-05 | The tool MUST support a `--dry-run` mode that parses and converts but makes no Fabric API calls. |
| CLI-06 | The tool MUST support a `--verbose` / `-v` flag that prints HTTP request/response details. |
| CLI-07 | The tool MUST support an `--output-dir` argument that writes all converted JSON artifacts to a local directory. |
| CLI-08 | The tool MUST support `--no-connections` to skip Fabric connection creation (uses dummy placeholder IDs). |
| CLI-09 | The tool MUST support `--no-dataflows` to skip Fabric Dataflow Gen2 creation. |
| CLI-10 | When invoked as `python -m ssis2fabric` or as the `ssis2fabric` console script (after `pip install`), behaviour MUST be identical. |

### 3.2 DTSX Parsing

| ID | Requirement |
|---|---|
| PAR-01 | The parser MUST read the package name, ID, description, and variable declarations from the DTSX XML. |
| PAR-02 | The parser MUST extract all Connection Managers with their type, server, database, file path, and URL properties. |
| PAR-03 | The parser MUST extract all top-level Executable elements and recursively extract executables nested inside ForEach Loop, For Loop, and Sequence containers. |
| PAR-04 | The parser MUST extract Precedence Constraints (From, To, EvalOp, Value, Expression) from both top-level and container-scoped constraint blocks. |
| PAR-05 | The parser MUST resolve task identity by both GUID and SSIS refId path (e.g. `Package\Task Name`) for precedence constraint resolution. |
| PAR-06 | The parser MUST detect the `DTS:Disabled="True"` attribute on any Executable and record it on the task model. |
| PAR-07 | The parser MUST support both new-format (attribute-based) and old-format (DTS:Property child-element-based) DTSX schemas. |
| PAR-08 | `User::` namespace variables MUST be extracted with their data type and default value. `System::` variables MUST be silently skipped. |

#### 3.2.1 Task-specific Parsing

| Task | Properties Extracted |
|---|---|
| Execute SQL | `SqlStatementSource`, `Connection`, `StoredProcedureName`, `ResultSet` |
| Data Flow | Full component graph (source, transformation, destination components with columns and properties) |
| Script | `Language` (CSharp / VB) |
| Send Mail | `To`, `From`, `CC`, `BCC`, `Subject`, `MessageSource` / `MessageBody`, `MessageSourceType`, `Priority`, `FileAttachments`, SMTP connection ref |
| Web Service | `WSDLFile`, `Service`, `WebMethod`, `Output`, `OutputType`, HTTP connection ref |
| FTP | `Operation`, `LocalPath`, `RemotePath`, `LocalVariable`, `RemoteVariable`, `IsLocalPathVariable`, `IsRemotePathVariable`, `OverwriteFileAtDestination`, `IsTransferTypeASCII`, `IsRecursive`, FTP connection ref |
| Execute Package | `PackageName` |
| Execute Process | `Executable`, `Arguments` |
| File System | `TaskOperationType` |
| Bulk Insert | Connection ref |
| ForEach Loop | Enumerator type, items expression, variable mapping, inner tasks |
| For Loop | `InitExpression`, `EvalExpression`, `AssignExpression`, inner tasks |
| Sequence Container | Inner tasks, inner precedence constraints |

### 3.3 Connection Manager Conversion

| ID | Requirement |
|---|---|
| CONN-01 | OLEDB and ADO.NET connection managers MUST be converted to Fabric `SQL (ShareableCloud)` connections. |
| CONN-02 | File and Flat File connection managers MUST be converted to Fabric `File` connections. |
| CONN-03 | HTTP connection managers MUST be converted to Fabric `Web (Anonymous)` connections. |
| CONN-04 | FTP connection managers MUST be converted to Fabric `FTP` connections. |
| CONN-05 | SMTP and unrecognised connection types MUST be converted to a dummy SQL connection with `skipTestConnection: true`. |
| CONN-06 | All connections MUST be created with placeholder/dummy credentials. The user MUST update credentials in Fabric after migration. |
| CONN-07 | Connection GUIDs returned by the Fabric API MUST be collected and passed to downstream pipeline/dataflow converters so that activities reference real connection IDs. |

### 3.4 Data Flow → Dataflow Gen2 Conversion

| ID | Requirement |
|---|---|
| DF-01 | Each SSIS Data Flow task MUST be converted to a separate Fabric Dataflow Gen2 item. |
| DF-02 | The M query generated MUST include a `Source` step using `Sql.Database("server", "db")` for OLEDB/ADO sources, `Csv.Document(File.Contents(...))` for flat file sources. |
| DF-03 | Transformation components MUST be mapped to their closest Power Query M equivalents (see mapping table). |
| DF-04 | Steps that require manual adjustment MUST include a `// TODO` annotation. |
| DF-05 | The dataflow definition MUST be base64-encoded and submitted as a Fabric item definition with the correct `payloadType`. |

#### 3.4.1 Component Mapping

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
| Destination | Comment only |

### 3.5 Pipeline Conversion

#### 3.5.1 General

| ID | Requirement |
|---|---|
| PIP-01 | A single Fabric Data Pipeline MUST be created per DTSX package. |
| PIP-02 | The pipeline display name MUST default to the SSIS package name and MUST be overrideable via `--pipeline-name`. |
| PIP-03 | SSIS `User::` variables MUST be converted to Fabric pipeline parameters with appropriate type mapping (see §3.5.3). |
| PIP-04 | Precedence constraints MUST be converted to Fabric `dependsOn` entries with the correct condition (`Succeeded`, `Failed`, `Completed`). |
| PIP-05 | Any task with `DTS:Disabled="True"` MUST be emitted with `state: InActive`, `onInactiveMarkAs: Succeeded`, and its description MUST be prefixed with `[Disabled in original SSIS package]`. |
| PIP-06 | Activities that cannot be fully converted MUST be emitted with `state: InActive` and `onInactiveMarkAs: Succeeded` so that the pipeline remains openable in Fabric. |

#### 3.5.2 Task → Activity Mapping

| SSIS Task | Fabric Activity | Active / InActive | Notes |
|---|---|---|---|
| Execute SQL (plain SQL) | `Script` | InActive | SQL statement placed in script body |
| Execute SQL (stored proc) | `SqlServerStoredProcedure` | InActive | SP name populated |
| Data Flow | `RefreshDataFlow` | Active (if DF created) | References Dataflow Gen2 by GUID |
| ForEach Loop Container | `ForEach` | Active | Inner activities recursively converted |
| For Loop Container | `Until` | InActive | Loop expressions emitted as TODO comment; SSIS runs WHILE condition is true, Fabric Until runs UNTIL condition is true |
| Sequence Container | `IfCondition` (`@bool(1)` always-true) | Active | Groups inner activities |
| Execute Package Task | `ExecutePipeline` | InActive | Referenced pipeline may not exist |
| Script Task | `Script` | InActive | Logic must be manually ported |
| Send Mail Task | `Office365Email` | InActive | From/To/CC/BCC/Subject/Body/Priority/Attachments populated; requires O365 connection |
| Web Service Task | `WebActivity` (POST) | InActive | WSDL URL, SOAPAction header, and envelope body must be completed manually |
| FTP Task – Receive | `Copy` (FTP → ADLS) | InActive | `FtpReadSettings` source, `AzureBlobFSWriteSettings` sink |
| FTP Task – Send | `Copy` (ADLS → FTP) | InActive | `AzureBlobFSReadSettings` source, `FtpWriteSettings` sink |
| FTP Task – DeleteRemoteFile / DeleteLocalFile | `Delete` | InActive | Store settings reflect local vs remote |
| FTP Task – directory ops / rename | `Script` | InActive | No direct Fabric equivalent |
| Execute Process Task | `WebActivity` | InActive | No direct equivalent |
| File System Task | `Script` | InActive | Rework using Lakehouse file APIs |
| Bulk Insert Task | `Copy` | InActive | Source/sink configuration required |
| All others | `Wait` (1 s) | InActive | Manual replacement required |

#### 3.5.3 Variable → Parameter Type Mapping

| SSIS Type | Fabric Parameter Type |
|---|---|
| String, DateTime | `string` |
| Int32, Int64 | `int` |
| Double, Decimal | `float` |
| Boolean | `bool` |

### 3.6 Authentication

| ID | Requirement |
|---|---|
| AUTH-01 | The tool MUST authenticate to Fabric using Microsoft Entra ID interactive browser authentication (`InteractiveBrowserCredential`). |
| AUTH-02 | The token MUST be cached in memory for the duration of the run. |
| AUTH-03 | In `--dry-run` mode, no authentication MUST be required. |

---

## 4. Non-Functional Requirements

| ID | Requirement |
|---|---|
| NFR-01 | The tool MUST be installable via `pip install <wheel>` on Python 3.9+. |
| NFR-02 | Runtime dependencies MUST be limited to `requests` and `azure-identity`. |
| NFR-03 | The tool MUST produce a human-readable summary of the parsed package (task count, connection count, data flow count) before conversion. |
| NFR-04 | All Fabric API errors MUST be surfaced with a descriptive message; the tool MUST exit with a non-zero code on fatal errors. |
| NFR-05 | Activity names MUST be sanitized to be valid Fabric activity names (max 100 characters, invalid characters replaced with `_`). |
| NFR-06 | When `--output-dir` is specified, all intermediate JSON artifacts MUST be written even if a subsequent API call fails. |

---

## 5. Output Artifacts

| File | Description |
|---|---|
| `connections.json` | Array of Fabric connection creation payloads |
| `dataflow_<name>.json` | Fabric Dataflow Gen2 item definition (one per SSIS Data Flow task) |
| `pipeline_<name>.json` | Fabric pipeline item definition (base64-encoded `pipeline-content.json` inside a `parts` wrapper) |

---

## 6. Distribution

| ID | Requirement |
|---|---|
| DIST-01 | The tool MUST be packaged as a Python wheel (`py3-none-any.whl`). |
| DIST-02 | The GitHub repository MUST include a GitHub Actions workflow that builds and publishes the wheel as a GitHub Release asset on every version tag (`v*.*.*`). |
| DIST-03 | The `dist/` directory MUST be excluded from version control via `.gitignore`. |

---

## 7. Post-Migration Checklist (User-Facing)

1. Open the pipeline in Fabric; activities with a ⚠ badge are `InActive` and need follow-up.
2. For each `InActive` activity, review the `description` field for the original SSIS task details.
3. For **For Loop** activities: rewrite the `Until` condition in Fabric expression syntax.
4. For **Send Mail** activities: configure an Office 365 connection and verify From/To addresses.
5. For **Web Service** activities: update the URL, SOAPAction header, and request body.
6. For **FTP Copy** activities: configure the FTP and ADLS linked services and datasets.
7. For **Script** activities: port the original C#/VB logic or replace with an equivalent Fabric activity.
8. For **ExecutePipeline** activities: verify the referenced pipeline has been migrated.
9. Open each Dataflow Gen2 and fix `// TODO` expressions; set real data source connections.
10. Update connection credentials via **Fabric > Manage connections and gateways**.
11. Run the pipeline in Debug mode and iterate.

---

## 8. Known Limitations (v0.1.0)

| Limitation | Impact |
|---|---|
| SSIS Script Task logic is not ported | Script activities are InActive placeholders; logic must be manually re-implemented |
| For Loop expressions use TODO placeholder | `Until` condition must be manually rewritten in Fabric expression language |
| SSIS expressions in Precedence Constraints are not evaluated | Expression-based constraints are converted with condition `Succeeded` |
| FTP linked service and datasets are not created | FTP Copy/Delete activities reference placeholder dataset names |
| SSIS project-level parameters/connections not supported | Only package-level connections and variables are processed |
| Credentials not migrated | All connections created with dummy/placeholder credentials |
| `MessageSourceType` Variable for Send Mail | Runtime-resolved mail bodies emit a TODO comment rather than a dynamic expression |

---

## 9. Future Considerations

- Support for SSIS project deployment model (`.ispac` files)
- SSIS expression parser to translate expressions to Fabric dynamic content syntax
- Automatic FTP linked service creation
- Test coverage / CI pipeline
- Publish to PyPI
- Support for additional SSIS task types: XML Task, Transfer Database Task, Analysis Services tasks
- OAuth/service-principal authentication mode (non-interactive)
