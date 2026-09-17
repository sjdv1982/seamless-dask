"""Use the shared lifecycle expiry controls from seamless-core tests."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path


_ROOT = Path(__file__).resolve().parents[3]
_SOURCE = _ROOT / "seamless-core" / "tests" / "helpers" / "reference_lifecycle.py"
_SPEC = importlib.util.spec_from_file_location("seamless_core_lifecycle_helper", _SOURCE)
assert _SPEC is not None and _SPEC.loader is not None
_MODULE = importlib.util.module_from_spec(_SPEC)
sys.modules[_SPEC.name] = _MODULE
_SPEC.loader.exec_module(_MODULE)

force_expiry = _MODULE.force_expiry
