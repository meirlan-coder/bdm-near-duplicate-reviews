"""Experiment parameters shared by every implementation.

Parameters are read from (in increasing priority):
  1. the defaults below,
  2. a JSON file (conf/lsh.json in the repository),
  3. environment variables named NDDUP_<FIELD> (upper case).

Hadoop Streaming tasks receive them through ``-cmdenv NDDUP_...`` and Spark
jobs through ``--params``, so a single JSON file drives both frameworks.
"""

import json
import os
from dataclasses import asdict, dataclass, fields


@dataclass(frozen=True)
class Params:
    shingle_k: int = 3          # word k-shingles
    min_tokens: int = 10        # reviews with fewer tokens are dropped in preprocessing
    num_bands: int = 20         # b
    rows_per_band: int = 5      # r   -> signature length n = b * r
    seed: int = 42              # seed of the MinHash hash-function family
    threshold: float = 0.8      # Jaccard threshold for a pair to be reported
    max_bucket: int = 500       # LSH buckets larger than this are skipped (and counted)

    @property
    def num_hashes(self) -> int:
        return self.num_bands * self.rows_per_band

    def lsh_threshold(self) -> float:
        """Approximate similarity at which P(candidate)=0.5: (1/b)^(1/r)."""
        return (1.0 / self.num_bands) ** (1.0 / self.rows_per_band)

    def candidate_probability(self, s: float) -> float:
        """P(pair with Jaccard s becomes a candidate) = 1 - (1 - s^r)^b."""
        return 1.0 - (1.0 - s ** self.rows_per_band) ** self.num_bands

    def to_env(self) -> dict:
        return {"NDDUP_" + k.upper(): str(v) for k, v in asdict(self).items()}

    def to_json(self) -> str:
        return json.dumps(asdict(self), sort_keys=True)


def load_params(path: str = None, env: dict = None) -> Params:
    env = os.environ if env is None else env
    values = {}
    path = path or env.get("NDDUP_PARAMS_FILE")
    if path and os.path.exists(path):
        with open(path, "r", encoding="utf-8") as fh:
            values.update(json.load(fh))
    for f in fields(Params):
        key = "NDDUP_" + f.name.upper()
        if key in env:
            values[f.name] = env[key]
    typed = {f.name: f.type(values[f.name]) for f in fields(Params) if f.name in values}
    return Params(**typed)
