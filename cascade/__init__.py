"""Cascade — a declarative, container-native pipeline tool.

This is the minimal reference implementation. A pipeline is a YAML file
declaring:

  - input:  named pipeline inputs, referenced in the dag via the ``$input`` sigil
  - types:  named record types with typed fields (the contracts)
  - refs:   executable artefacts — here, just a pre-built container ``image``
            plus a typed input/output contract
  - dags:   named, reusable subdags (optional)
  - dag:    the root topology; nodes reference refs, declare ``depends_on``
            edges, and may ``scatter`` over an upstream collection

The implementation is split along the protocol's boundaries:

  model.py   — the data model (the YAML, parsed)
  loader.py  — YAML -> model
  types.py   — the type system: parsing type expressions, subtyping, edge checks
  validate.py— validate-dags (and a thin validate-refs)
  plan.py    — resolve subdags + topological sort -> an execution plan
  store.py   — the data plane: put/get by key (filesystem or S3)
  runner.py  — launch a container (subprocess docker run), pointers in/out
  engine.py  — the coordinator: walk waves, expand scatter, thread keys
  cli.py     — the ``cascade`` command
"""

from .loader import load_pipeline
from .model import Pipeline
from .validate import validate_dags, validate_refs, ValidationReport
from .plan import build_plan, ExecutionPlan
from . import hooks

__all__ = [
    "load_pipeline",
    "Pipeline",
    "validate_dags",
    "validate_refs",
    "ValidationReport",
    "build_plan",
    "ExecutionPlan",
    "hooks",
]

__version__ = "0.1.0"
