#!/usr/bin/env python3
"""Quality evaluation on the 'sample' dataset against exact all-pairs.

1. Runs the MapReduce and the Spark pipeline on the sample (on the cluster),
   keeping the candidate pairs of both.
2. Computes the exact Jaccard similarity of *all* n(n-1)/2 pairs of the
   sample (bench/exact_allpairs.py) -> ground truth.
3. Reports, for the explicit threshold t (conf/lsh.json):
     * number of truly similar pairs (J >= t)
     * candidate recall = |candidates ∩ truth| / |truth|
     * final recall / precision of the reported pairs
     * reduction in pairwise comparisons = 1 - |candidates| / (n(n-1)/2)
   plus the empirical probability of becoming a candidate per Jaccard bin
   compared with the theoretical S-curve 1-(1-s^r)^b, and a small (b, r)
   sensitivity study computed locally with the same hash functions.

    python3 bench/evaluate.py
"""
import json
import os
import sys
import time
from collections import defaultdict

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(HERE, "..", "src"))
import pipelines as pl  # noqa: E402
from exact_allpairs import all_pairs, read_prep  # noqa: E402
from nddup import load_params  # noqa: E402
from nddup.core import MinHasher, band_keys, shingle_hashes  # noqa: E402

OUT = os.path.join(pl.PROJECT, "results", "eval")
BINS = [0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0001]


def pairs_of(lines):
    return {tuple(l.split("\t")[:2]) for l in lines}


def lsh_candidates(rids, texts, p, b, r, mh):
    sigs = [mh.signature(shingle_hashes(t, p.shingle_k)) for t in texts]
    cands = set()
    buckets = defaultdict(list)
    for rid, sig in zip(rids, sigs):
        for key in band_keys(sig[: b * r], b, r):
            buckets[key].append(rid)
    for ids in buckets.values():
        if 1 < len(ids) <= p.max_bucket:
            ids = sorted(ids)
            for i in range(len(ids)):
                for j in range(i + 1, len(ids)):
                    cands.add((ids[i], ids[j]))
    return cands


def main():
    os.makedirs(OUT, exist_ok=True)
    pl.build_zip()
    p = load_params(pl.PARAMS_FILE)
    log = os.path.join(OUT, "eval.log")
    raw = pl.HDFS_ROOT + "/raw/sample"
    runs = {}
    for fw in ("mr", "spark"):
        work = "%s/work/eval-%s" % (pl.HDFS_ROOT, fw)
        stages = pl.run_pipeline(fw, raw, work, "sample", save_candidates=True, log=log)
        runs[fw] = {
            "seconds_prep": sum(j["seconds"] for j in stages["prep"]),
            "seconds_detect": sum(j["seconds"] for j in stages["detect"]),
            "counters": pl.summarize_counters(fw, stages),
            "candidates": pairs_of(pl.hdfs_lines(work + "/pairs")),
            "similar": pairs_of(pl.hdfs_lines(work + "/similar")),
        }
        print("[eval] %s: %d candidates, %d similar" % (fw, len(runs[fw]["candidates"]),
                                                       len(runs[fw]["similar"])), flush=True)

    local_prep = os.path.join(OUT, "sample_prep.tsv")
    if os.path.exists(local_prep):
        os.remove(local_prep)
    pl.hdfs_getmerge(pl.HDFS_ROOT + "/work/eval-mr/prep", local_prep)
    rids, texts = read_prep([local_prep])
    n = len(rids)
    t0 = time.time()
    exact = all_pairs(rids, texts, p.shingle_k, BINS[0])
    exact_seconds = time.time() - t0
    jac = {(a, b): s for a, b, s in exact}
    truth = {pair for pair, s in jac.items() if s >= p.threshold}
    total_pairs = n * (n - 1) // 2
    print("[eval] exact all-pairs: n=%d, %d pairs compared in %.1fs, %d pairs with J>=%.2f"
          % (n, total_pairs, exact_seconds, len(truth), p.threshold), flush=True)

    summary = {"params": json.loads(p.to_json()), "reviews": n, "all_pairs": total_pairs,
               "exact_seconds": exact_seconds, "true_pairs": len(truth),
               "lsh_threshold_estimate": p.lsh_threshold(),
               "theoretical_recall_at_threshold": p.candidate_probability(p.threshold),
               "frameworks": {}}
    for fw, rr in runs.items():
        C, S = rr["candidates"], rr["similar"]
        summary["frameworks"][fw] = {
            "candidate_pairs": len(C),
            "reported_pairs": len(S),
            "candidate_recall": len(C & truth) / len(truth) if truth else None,
            "final_recall": len(S & truth) / len(truth) if truth else None,
            "final_precision": len(S & truth) / len(S) if S else None,
            "comparison_reduction": 1 - len(C) / float(total_pairs),
            "candidate_precision": len(C & truth) / len(C) if C else None,
            "seconds_prep": rr["seconds_prep"], "seconds_detect": rr["seconds_detect"],
            "counters": rr["counters"],
        }
    summary["identical_outputs"] = runs["mr"]["similar"] == runs["spark"]["similar"]
    summary["identical_candidates"] = runs["mr"]["candidates"] == runs["spark"]["candidates"]

    # empirical S-curve
    C = runs["mr"]["candidates"]
    curve = []
    for lo, hi in zip(BINS[:-1], BINS[1:]):
        in_bin = [pair for pair, s in jac.items() if lo <= s < hi]
        mid = (lo + min(hi, 1.0)) / 2
        curve.append({"bin": "[%.1f, %.1f%s" % (lo, min(hi, 1.0), "]" if hi > 1 else ")"),
                      "pairs": len(in_bin),
                      "candidates": sum(1 for x in in_bin if x in C),
                      "empirical_probability": (sum(1 for x in in_bin if x in C) / len(in_bin)) if in_bin else None,
                      "theoretical_probability_mid": p.candidate_probability(mid)})
    summary["s_curve"] = curve

    # (b, r) sensitivity with the same n = b*r hash functions
    mh = MinHasher(p.num_hashes, p.seed)
    sens = []
    for b, r in [(50, 2), (25, 4), (20, 5), (10, 10), (5, 20)]:
        if b * r > p.num_hashes:
            continue
        c = lsh_candidates(rids, texts, p, b, r, mh)
        sens.append({"b": b, "r": r, "threshold_estimate": (1.0 / b) ** (1.0 / r),
                     "candidate_pairs": len(c),
                     "candidate_recall": len(c & truth) / len(truth) if truth else None,
                     "comparison_reduction": 1 - len(c) / float(total_pairs)})
    summary["band_sensitivity"] = sens

    # a few example pairs for the report
    text_of = dict(zip(rids, texts))
    examples = []
    for lo, hi in [(0.8, 0.9), (0.9, 0.999), (1.0, 1.01)]:
        for pair in sorted(x for x in truth if lo <= jac[x] < hi)[:3]:
            examples.append({"a": pair[0], "b": pair[1], "jaccard": jac[pair],
                             "text_a": text_of[pair[0]][:400], "text_b": text_of[pair[1]][:400]})
    summary["examples"] = examples

    with open(os.path.join(OUT, "summary.json"), "w") as fh:
        json.dump(summary, fh, indent=2)
    with open(os.path.join(OUT, "exact_pairs.tsv"), "w") as fh:
        for (a, b), s in sorted(jac.items()):
            fh.write("%s\t%s\t%.6f\n" % (a, b, s))
    print(json.dumps({k: v for k, v in summary.items() if k not in ("examples", "s_curve")}, indent=2))


if __name__ == "__main__":
    main()
