"""Thread and JIT-target pins that make fits deterministic.

`apply` must run before NumPy, SciPy or Numba is imported: their thread
pools and Numba's target CPU are read from the environment at import time.
The package `__init__` calls it, so importing anything from the package is
enough. The values are forced, not defaulted: determinism is part of the
HTTP contract, not a tuning knob.
"""

import os

PINS: dict[str, str] = {
    "OMP_NUM_THREADS": "1",
    "OPENBLAS_NUM_THREADS": "1",
    "MKL_NUM_THREADS": "1",
    "NUMBA_NUM_THREADS": "1",
    "NUMBA_CPU_NAME": "generic",
}


class PinError(Exception):
    """A thread pool is not pinned to one thread."""


def apply() -> None:
    for name, value in PINS.items():
        os.environ[name] = value


def verify() -> None:
    """Refuse to serve if Numba would use more than one thread."""
    import numba

    threads = numba.get_num_threads()
    if threads != 1:
        raise PinError(f"numba uses {threads} threads; the sidecar requires exactly 1")
