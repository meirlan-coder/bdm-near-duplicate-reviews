import json
import os
import sys

import numpy as np

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from nddup import (MinHasher, Params, band_keys, bucket_pairs, jaccard,  # noqa: E402
                   load_params, normalize, preprocess_line, shingle_hashes)


def test_normalize_strips_html_and_punctuation():
    assert normalize("Great <br />product!!  Works&amp;fits_well") == "great product works fits well"
    assert normalize("ＦＵＬＬ width") == "full width"


def test_preprocess_filters_short_reviews():
    p = Params(min_tokens=4)
    line = json.dumps({"review_id": "R1", "title": "Nice", "text": "a b"})
    assert preprocess_line(line, p) is None
    line = json.dumps({"review_id": "R1", "title": "Nice", "text": "a b c d"})
    assert preprocess_line(line, p) == "R1\tnice a b c d"
    assert preprocess_line("not json", p) is None


def test_shingles_are_deterministic_and_unique():
    a = shingle_hashes("the cat sat on the mat the cat sat", 3)
    b = shingle_hashes("the cat sat on the mat the cat sat", 3)
    assert a.dtype == np.uint32
    assert np.array_equal(a, b)
    assert len(a) == len(set(a.tolist())) == 6   # 'the cat sat' repeated


def test_jaccard():
    a = shingle_hashes("one two three four five six", 2)
    b = shingle_hashes("one two three four five seven", 2)
    assert jaccard(a, a) == 1.0
    assert abs(jaccard(a, b) - 4 / 6) < 1e-12


def test_minhash_estimates_jaccard():
    rng = np.random.RandomState(0)
    universe = np.unique(rng.randint(0, 2 ** 32, size=4100, dtype=np.int64))[:4000].astype(np.uint64)
    rng.shuffle(universe)
    a = np.unique(universe[:1000])
    b = np.unique(universe[300:1300])
    true = jaccard(a, b)
    mh = MinHasher(400, 42)
    est = float(np.mean(mh.signature(a) == mh.signature(b)))
    assert abs(est - true) < 0.08


def test_minhasher_reproducible():
    s = shingle_hashes("a b c d e f g h i j k", 3)
    assert np.array_equal(MinHasher(100, 42).signature(s), MinHasher(100, 42).signature(s))
    assert not np.array_equal(MinHasher(100, 42).signature(s), MinHasher(100, 43).signature(s))


def test_identical_docs_share_all_bands():
    p = Params()
    mh = MinHasher(p.num_hashes, p.seed)
    s = shingle_hashes("x y z w v u t s r q p", 3)
    k1 = band_keys(mh.signature(s), p.num_bands, p.rows_per_band)
    k2 = band_keys(mh.signature(s.copy()), p.num_bands, p.rows_per_band)
    assert k1 == k2 and len(set(k1)) == p.num_bands


def test_bucket_pairs():
    assert bucket_pairs(["R2", "R1", "R3"], 10) == [("R1", "R2"), ("R1", "R3"), ("R2", "R3")]
    assert bucket_pairs(["R1"], 10) == []
    assert bucket_pairs(["R1", "R1"], 10) == []
    assert bucket_pairs(["R%d" % i for i in range(11)], 10) is None


def test_s_curve():
    p = Params(num_bands=20, rows_per_band=5)
    assert p.candidate_probability(0.8) > 0.999
    assert abs(p.lsh_threshold() - 0.5493) < 1e-3


def test_load_params_env_overrides(tmp_path):
    f = tmp_path / "p.json"
    f.write_text(json.dumps({"num_bands": 10, "threshold": 0.7}))
    p = load_params(str(f), env={"NDDUP_ROWS_PER_BAND": "4"})
    assert (p.num_bands, p.rows_per_band, p.threshold) == (10, 4, 0.7)
    assert load_params(None, env=p.to_env()) == p
