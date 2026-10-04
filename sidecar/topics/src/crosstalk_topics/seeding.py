"""UMAP's random state from a u64 seed (docs/features/topics_sidecar.md,
"Determinism").

A plain `RandomState(seed)` takes only 32 bits; a `SeedSequence` takes the
whole u64, so seeds differing only in their high bits give different
layouts. Both are built fresh per request: UMAP consumes the generator.
"""

import numpy as np


def random_state(seed: int) -> np.random.RandomState:
    return np.random.RandomState(np.random.MT19937(np.random.SeedSequence(seed)))


def transform_seed(seed: int) -> int:
    """UMAP's `transform_seed`: the first 32-bit word the seed's sequence
    generates."""
    return int(np.random.SeedSequence(seed).generate_state(1)[0])
