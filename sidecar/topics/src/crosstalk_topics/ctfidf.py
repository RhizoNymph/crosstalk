"""Class-based TF-IDF labels (docs/features/topics_sidecar.md, step 4).

Each class's term counts are L1-normalized; a term's weight is
`tf * ln(1 + A / f)`, where `A` is the mean number of counted terms per
class and `f` the term's total count over every class. A class's terms are
those with a positive weight, highest first, ties to the smaller term.
"""

import numpy as np
from scipy import sparse
from sklearn.feature_extraction.text import CountVectorizer

type Terms = list[tuple[str, float]]


def class_terms(documents: list[str], top_terms: int) -> list[Terms]:
    """The top terms of each class; `documents[k]` is class `k`'s text."""
    if not documents:
        return []
    vectorizer = CountVectorizer(lowercase=True, stop_words="english")
    try:
        counts = sparse.csr_matrix(vectorizer.fit_transform(documents), dtype=np.float64)
    except ValueError:
        # Every token is a stop word or too short: there is no vocabulary.
        return [[] for _ in documents]
    vocabulary = vectorizer.get_feature_names_out()
    classes = counts.shape[0]
    mean_terms = counts.sum() / classes
    frequency = np.asarray(counts.sum(axis=0)).ravel()
    idf = np.log1p(mean_terms / frequency)
    row_totals = np.asarray(counts.sum(axis=1)).ravel()
    scale = np.divide(1.0, row_totals, out=np.zeros_like(row_totals), where=row_totals > 0)
    weights = sparse.csr_matrix(sparse.diags(scale) @ counts @ sparse.diags(idf))
    result: list[Terms] = []
    for row in range(classes):
        start, end = weights.indptr[row], weights.indptr[row + 1]
        entries = [
            (str(vocabulary[index]), float(weight))
            for index, weight in zip(
                weights.indices[start:end], weights.data[start:end], strict=True
            )
            if weight > 0.0
        ]
        entries.sort(key=lambda entry: (-entry[1], entry[0]))
        result.append(entries[:top_terms])
    return result
