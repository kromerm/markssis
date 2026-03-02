"""
SSIS data models representing parsed DTSX package components.
"""
from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any


@dataclass
class SSISConnectionManager:
    """Represents an SSIS Connection Manager."""
    id: str
    name: str
    connection_type: str          # OLEDB, ADO.NET, FILE, HTTP, FTP, SMTP, etc.
    connection_string: Optional[str] = None
    server_name: Optional[str] = None
    database_name: Optional[str] = None
    file_path: Optional[str] = None
    url: Optional[str] = None
    properties: Dict[str, Any] = field(default_factory=dict)


@dataclass
class SSISPrecedenceConstraint:
    """Represents a precedence constraint (edge) between tasks."""
    from_task: str
    to_task: str
    eval_op: str = "Constraint"   # Constraint, Expression, ExpressionAndConstraint, ExpressionOrConstraint
    value: int = 0                # 0=Success, 1=Failure, 2=Completion
    expression: Optional[str] = None


@dataclass
class SSISTask:
    """Represents a single SSIS control flow task."""
    id: str
    name: str
    task_type: str                # ExecuteSQL, DataFlow, Script, ForEachLoop, etc.
    description: Optional[str] = None
    connection_ref: Optional[str] = None    # Connection manager name or id
    properties: Dict[str, Any] = field(default_factory=dict)
    disabled: bool = False


@dataclass
class SSISForEachLoop:
    """Represents a ForEach Loop container."""
    id: str
    name: str
    enumerator_type: str          # File, ItemEnumerator, ADO, etc.
    items_expression: Optional[str] = None
    variable_name: Optional[str] = None
    tasks: List["SSISTask"] = field(default_factory=list)
    precedence_constraints: List[SSISPrecedenceConstraint] = field(default_factory=list)
    properties: Dict[str, Any] = field(default_factory=dict)
    disabled: bool = False


@dataclass
class SSISForLoop:
    """Represents a For Loop container."""
    id: str
    name: str
    init_expression: Optional[str] = None      # e.g. "@i = 0"
    eval_expression: Optional[str] = None      # e.g. "@i < 10"  (loop WHILE true)
    assign_expression: Optional[str] = None    # e.g. "@i = @i + 1"
    tasks: List["SSISTask"] = field(default_factory=list)
    precedence_constraints: List["SSISPrecedenceConstraint"] = field(default_factory=list)
    description: Optional[str] = None
    disabled: bool = False


@dataclass
class SSISSequenceContainer:
    """Represents a Sequence Container."""
    id: str
    name: str
    tasks: List["SSISTask"] = field(default_factory=list)
    precedence_constraints: List[SSISPrecedenceConstraint] = field(default_factory=list)
    disabled: bool = False


@dataclass
class SSISDataFlowComponent:
    """A single component within an SSIS Data Flow."""
    id: str
    name: str
    component_class: str          # e.g. Microsoft.OLEDBSource, Microsoft.OLEDBDestination
    connection_ref: Optional[str] = None
    sql_command: Optional[str] = None
    table_name: Optional[str] = None
    properties: Dict[str, Any] = field(default_factory=dict)
    input_columns: List[Dict[str, Any]] = field(default_factory=list)
    output_columns: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class SSISDataFlowPath:
    """A path (edge) between two data flow components."""
    id: str
    name: str
    start_id: str
    end_id: str


@dataclass
class SSISDataFlow:
    """Represents an SSIS Data Flow task (pipeline task containing data flow)."""
    id: str
    name: str
    components: List[SSISDataFlowComponent] = field(default_factory=list)
    paths: List[SSISDataFlowPath] = field(default_factory=list)


@dataclass
class SSISVariable:
    """Represents an SSIS package variable."""
    id: str
    name: str
    namespace: str = "User"
    data_type: str = "String"
    value: Optional[str] = None


@dataclass
class SSISPackage:
    """Top-level SSIS package model."""
    name: str
    id: str
    description: Optional[str] = None
    connection_managers: List[SSISConnectionManager] = field(default_factory=list)
    tasks: List[Any] = field(default_factory=list)          # SSISTask | SSISForEachLoop | SSISSequenceContainer
    precedence_constraints: List[SSISPrecedenceConstraint] = field(default_factory=list)
    data_flows: List[SSISDataFlow] = field(default_factory=list)
    variables: List[SSISVariable] = field(default_factory=list)
