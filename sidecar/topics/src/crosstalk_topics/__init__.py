"""crosstalk-topics: topic modeling and UMAP layouts for crosstalk, over HTTP.

Importing the package pins every numeric thread pool to one thread and
Numba's JIT target to a generic CPU, before NumPy or Numba is imported, so a
fit is deterministic (docs/features/topics_sidecar.md, "Determinism").
"""

from crosstalk_topics import pins as _pins

_pins.apply()
