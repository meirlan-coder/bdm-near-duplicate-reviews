"""Core near-duplicate detection logic (framework independent).

Pipeline for one review
-----------------------
raw JSON line --parse_review--> (review_id, title + text)
              --normalize-----> lower-cased tokens separated by single spaces
              --shingle_hashes-> sorted unique uint32 array of CRC32 hashes
                                 of the word k-shingles
              --MinHasher------> signature of n = b*r MinHash values
              --band_keys------> b bucket keys (one per band)

Pairs of reviews sharing at least one bucket key become *candidate pairs*;
each candidate pair is verified with the exact Jaccard similarity of the two
shingle sets and reported if J >= threshold.

Everything here is deterministic across processes and machines: Python's
built-in ``hash`` (which is randomised per process) is never used.
"""

import hashlib
import html
import json
import re
import unicodedata
import zlib
from itertools import combinations

import numpy as np

# Largest prime below 2**32.  With a, b < P and x < 2**32 the expression
# a*x + b < 2**64, so it can be evaluated exactly in uint64 arithmetic.
MERSENNE_LIKE_PRIME = 4294967291

_TAG_RE = re.compile(r"<[^>]+>")
_NONWORD_RE = re.compile(r"[\W_]+", re.UNICODE)


# --------------------------------------------------------------------------
# Parsing and normalisation (= the "preprocessing" stage)
# --------------------------------------------------------------------------
def parse_review(line):
    """Parse one JSON line of the Amazon Reviews 2023 dump.

    Returns (review_id, text) or None for malformed lines.  ``review_id`` is
    added by scripts/prepare_data.py; the text is ``title + ' ' + text``.
    """
    line = line.strip()
    if not line:
        return None
    try:
        obj = json.loads(line)
    except ValueError:
        return None
    rid = obj.get("review_id")
    if not rid:
        return None
    text = "%s %s" % (obj.get("title") or "", obj.get("text") or "")
    return rid, text


def normalize(text):
    """HTML-unescape, strip tags, NFKC, lower-case, keep word characters only."""
    text = html.unescape(text)
    text = _TAG_RE.sub(" ", text)
    text = unicodedata.normalize("NFKC", text).lower()
    text = _NONWORD_RE.sub(" ", text)
    return " ".join(text.split())


def make_record(rid, norm_text):
    """Serialise a preprocessed review as one TSV line (the HDFS format)."""
    return "%s\t%s" % (rid, norm_text)


def split_record(line):
    rid, _, text = line.rstrip("\n").partition("\t")
    return rid, text


def preprocess_line(line, params):
    """raw JSON line -> TSV record, or None if malformed / too short."""
    parsed = parse_review(line)
    if parsed is None:
        return None
    rid, text = parsed
    norm = normalize(text)
    if norm.count(" ") + 1 < params.min_tokens or not norm:
        return None
    return make_record(rid, norm)


# --------------------------------------------------------------------------
# Shingling and similarity
# --------------------------------------------------------------------------
def shingle_hashes(norm_text, k):
    """Sorted unique uint32 array with the CRC32 hash of every word k-shingle."""
    tokens = norm_text.split(" ")
    if len(tokens) < k:
        shingles = {" ".join(tokens)}
    else:
        shingles = {" ".join(tokens[i:i + k]) for i in range(len(tokens) - k + 1)}
    hashes = np.fromiter((zlib.crc32(s.encode("utf-8")) for s in shingles),
                         dtype=np.uint32, count=len(shingles))
    return np.unique(hashes)


def jaccard(a, b):
    """Exact Jaccard similarity of two sorted unique hash arrays."""
    inter = np.intersect1d(a, b, assume_unique=True).size
    union = a.size + b.size - inter
    return float(inter) / union if union else 1.0


# --------------------------------------------------------------------------
# MinHash + LSH
# --------------------------------------------------------------------------
class MinHasher(object):
    """Family of n universal hash functions h_i(x) = (a_i x + b_i) mod P.

    The coefficients depend only on (seed, n), so every mapper, reducer and
    Spark executor builds exactly the same functions.
    """

    def __init__(self, num_hashes, seed):
        rng = np.random.RandomState(seed)
        p = MERSENNE_LIKE_PRIME
        self.a = rng.randint(1, p, size=num_hashes, dtype=np.int64).astype(np.uint64)
        self.b = rng.randint(0, p, size=num_hashes, dtype=np.int64).astype(np.uint64)
        self.p = np.uint64(p)
        self.num_hashes = num_hashes

    def signature(self, shingles):
        """MinHash signature (uint64 array of length n) of a shingle-hash array."""
        x = shingles.astype(np.uint64)
        values = (self.a[:, None] * x[None, :] + self.b[:, None]) % self.p
        return values.min(axis=1)


def band_keys(signature, num_bands, rows_per_band):
    """One bucket key per band: 'BB' band index + 16 hex chars of MD5(band)."""
    keys = []
    sig = signature.astype("<u8")
    for band in range(num_bands):
        chunk = sig[band * rows_per_band:(band + 1) * rows_per_band].tobytes()
        digest = hashlib.md5(band.to_bytes(2, "little") + chunk).hexdigest()[:16]
        keys.append("%02d%s" % (band, digest))
    return keys


def bucket_pairs(review_ids, max_bucket):
    """All pairs (a, b) with a < b inside one LSH bucket.

    Returns None when the bucket holds more than ``max_bucket`` reviews (the
    caller counts such buckets; they are skipped to avoid a quadratic blow-up
    caused by large groups of identical template texts).
    """
    ids = sorted(set(review_ids))
    if len(ids) < 2:
        return []
    if len(ids) > max_bucket:
        return None
    return list(combinations(ids, 2))
