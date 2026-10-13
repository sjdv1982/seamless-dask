"""HashType validation helpers for Seamless-specific celltypes."""

from seamless.checksum.deep import DEEP_CELLTYPES


def hash_validation_celltype(celltype: str) -> str:
    """Return the stored celltype used for checksum-level validation.

    Deep celltypes are represented by plain JSON index buffers. Their shape and
    members are validated by Seamless's deep-structure handling, not HashType.
    """
    if celltype in DEEP_CELLTYPES:
        return "plain"
    return celltype
