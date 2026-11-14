#!/usr/bin/env python3
"""Exact all-pairs Jaccard similarity (the brute-force baseline).

Every pair (i, j), i < j, of the sample is compared: the shingle sets are put
in a sparse binary matrix X (reviews x shingles) and, block by block of rows,
the intersection sizes |A ∩ B| = (X Xᵀ)_ij are computed with sparse matrix
products.  Pairs without any common shingle have J = 0 and are not stored.
The result is mathematically identical to looping over all n(n-1)/2 pairs.

Only pairs with J >= --min-jaccard are written (default 0.3, enough to draw
the LSH S-curve), as 'a<TAB>b<TAB>J'.
"""
import argparse
import json
import os
import sys
import time

import numpy as np
from scipy import sparse

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "src"))
from nddup import load_params  # noqa: E402
from nddup.core import shingle_hashes, split_record  # noqa: E402


def read_prep(paths):
    rids, texts = [], []
    for path in paths:
        with open(path, encoding="utf-8") as fh:
            for line in fh:
                rid, text = split_record(line)
                if text:
                    rids.append(rid)
                    texts.append(text)
    order = np.argsort(rids)
    return [rids[i] for i in order], [texts[i] for i in order]


def all_pairs(rids, texts, k, min_j, block=500):
    sets = [shingle_hashes(t, k) for t in texts]
    vocab = {}
    rows, cols = [], []
    for i, s in enumerate(sets):
        for h in s.tolist():
            cols.append(vocab.setdefault(h, len(vocab)))
            rows.append(i)
    n = len(sets)
    X = sparse.csr_matrix((np.ones(len(rows), dtype=np.int32), (rows, cols)), shape=(n, len(vocab)))
    sizes = np.asarray(X.sum(axis=1)).ravel()
    XT = X.T.tocsc()
    out = []
    for start in range(0, n, block):
        stop = min(n, start + block)
        inter = (X[start:stop] @ XT).tocoo()
        i = inter.row + start
        j = inter.col
        keep = j > i
        i, j, v = i[keep], j[keep], inter.data[keep]
        jac = v / (sizes[i] + sizes[j] - v)
        sel = jac >= min_j
        for a, b, s in zip(i[sel], j[sel], jac[sel]):
            out.append((rids[a], rids[b], float(s)))
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--prep", nargs="+", required=True, help="preprocessed TSV part files")
    ap.add_argument("--out", required=True)
    ap.add_argument("--params", default=None)
    ap.add_argument("--min-jaccard", type=float, default=0.3)
    args = ap.parse_args()
    p = load_params(args.params)
    t0 = time.time()
    rids, texts = read_prep(args.prep)
    pairs = all_pairs(rids, texts, p.shingle_k, args.min_jaccard)
    elapsed = time.time() - t0
    with open(args.out, "w") as fh:
        for a, b, s in pairs:
            fh.write("%s\t%s\t%.6f\n" % (a, b, s))
    n = len(rids)
    info = {"reviews": n, "all_pairs": n * (n - 1) // 2, "pairs_ge_min": len(pairs),
            "min_jaccard": args.min_jaccard, "seconds": elapsed}
    with open(args.out + ".json", "w") as fh:
        json.dump(info, fh, indent=2)
    print(json.dumps(info))


if __name__ == "__main__":
    main()
