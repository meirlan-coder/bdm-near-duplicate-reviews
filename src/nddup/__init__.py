"""nddup - near-duplicate detection with MinHash + LSH.

This package contains the *only* implementation of the text logic used by the
project: parsing, normalisation, shingling, hashing, MinHash signatures, LSH
banding and Jaccard similarity.  Both the Hadoop Streaming (MapReduce) jobs and
the PySpark jobs import it, so the two frameworks are guaranteed to apply the
same preprocessing, the same hash functions and the same similarity threshold.
"""

from .params import Params, load_params  # noqa: F401
from .core import (  # noqa: F401
    parse_review,
    preprocess_line,
    normalize,
    shingle_hashes,
    MinHasher,
    band_keys,
    bucket_pairs,
    jaccard,
    make_record,
    split_record,
)

__version__ = "1.0.0"
