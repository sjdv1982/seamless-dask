"""Typed payloads shared between the Seamless Dask client and callers."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Mapping, MutableMapping, Optional

from distributed import Future


@dataclass
class TransformationInputSpec:
    """Describe a single transformation input for Dask submission."""

    name: str
    celltype: str
    subcelltype: Optional[str]
    checksum: Optional[str]
    kind: str = "checksum"  # "checksum" | "transformation"


@dataclass
class TransformationSubmission:
    """Bundle the information required to submit a transformation to Dask."""

    transformation_dict: Dict[str, Any]
    inputs: Mapping[str, TransformationInputSpec]
    input_futures: Mapping[str, Future]
    tf_checksum: Optional[str]
    tf_dunder: Dict[str, Any]
    scratch: bool
    meta: MutableMapping[str, Any] = field(default_factory=dict)
    require_value: bool = False
    allow_input_fingertip: bool = False


@dataclass
class TransformationFutures:
    """Collection of the Dask futures for a transformation."""

    base: Future
    fat: Future | None
    thin: Future
    tf_checksum: Optional[str] = None
    result_checksum: Optional[str] = None
