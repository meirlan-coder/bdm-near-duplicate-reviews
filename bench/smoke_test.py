#!/usr/bin/env python3
"""Smoke test on the cluster: 20k synthetic reviews with planted duplicates.

Checks that (1) MapReduce and Spark produce byte-identical results and
(2) the result equals the exact all-pairs ground truth.  Takes a few minutes.
"""
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(HERE, "..", "src"))
import pipelines as pl  # noqa: E402
from exact_allpairs import all_pairs, read_prep  # noqa: E402
from nddup import load_params  # noqa: E402


def main():
    pl.build_zip()
    p = load_params(pl.PARAMS_FILE)
    log = os.path.join(pl.PROJECT, "results", "logs", "smoke.log")
    os.makedirs(os.path.dirname(log), exist_ok=True)
    out = {}
    for fw in ("mr", "spark"):
        work = "%s/work/smoke-%s" % (pl.HDFS_ROOT, fw)
        st = pl.run_pipeline(fw, pl.HDFS_ROOT + "/raw/smoke", work, "smoke", log=log)
        out[fw] = pl.hdfs_lines(work + "/similar")
        print("[smoke] %-5s prep %.1fs detect %.1fs -> %d similar pairs" % (
            fw, sum(j["seconds"] for j in st["prep"]), sum(j["seconds"] for j in st["detect"]),
            len(out[fw])), flush=True)
    local = os.path.join(pl.PROJECT, "results", "tmp", "smoke_prep.tsv")
    os.makedirs(os.path.dirname(local), exist_ok=True)
    if os.path.exists(local):
        os.remove(local)
    pl.hdfs_getmerge(pl.HDFS_ROOT + "/work/smoke-mr/prep", local)
    rids, texts = read_prep([local])
    truth = {(a, b) for a, b, s in all_pairs(rids, texts, p.shingle_k, p.threshold)}
    got = {tuple(l.split("\t")[:2]) for l in out["mr"]}
    same = out["mr"] == out["spark"]
    recall = len(got & truth) / float(len(truth)) if truth else 1.0
    print("[smoke] MR == Spark: %s | true pairs %d | recall %.4f | false positives %d"
          % (same, len(truth), recall, len(got - truth)), flush=True)
    ok = same and not (got - truth) and recall > 0.95
    print("[smoke] %s" % ("PASSED" if ok else "FAILED"), flush=True)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
