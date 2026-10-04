"""The contract's matrix: little-endian binary32 values, row-major, as
lower-case hex (docs/features/topics_sidecar.md, "Matrices")."""

import re

import numpy as np
import numpy.typing as npt

from crosstalk_topics.errors import InvalidRequest
from crosstalk_topics.wire import Matrix

_HEX = re.compile(r"[0-9a-f]*")
_DTYPE = np.dtype("<f4")

type F32Array = npt.NDArray[np.float32]


def decode(matrix: Matrix, field: str) -> F32Array:
    """The `rows x columns` float32 array `matrix` holds."""
    expected = matrix.rows * matrix.columns * 8
    if len(matrix.data) != expected:
        raise InvalidRequest(
            f"{field}.data: expected {expected} hex digits for {matrix.rows}x{matrix.columns}, "
            f"got {len(matrix.data)}"
        )
    if _HEX.fullmatch(matrix.data) is None:
        raise InvalidRequest(f"{field}.data: not lower-case hex")
    values = np.frombuffer(bytes.fromhex(matrix.data), dtype=_DTYPE)
    array = values.reshape(matrix.rows, matrix.columns).astype(np.float32)
    if not bool(np.isfinite(array).all()):
        raise InvalidRequest(f"{field}.data: holds a NaN or an infinity")
    return array


def encode(array: npt.NDArray[np.floating]) -> Matrix:
    """`array` (two-dimensional) as a matrix of binary32 values."""
    if array.ndim != 2:
        raise ValueError(f"a matrix is two-dimensional, got {array.ndim} dimensions")
    rows, columns = array.shape
    data = np.ascontiguousarray(array, dtype=_DTYPE).tobytes().hex()
    return Matrix(rows=rows, columns=columns, data=data)
