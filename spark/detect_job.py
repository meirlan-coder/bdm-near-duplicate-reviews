#!/usr/bin/env python3
"""Spark job D: near-duplicate detection with a custom MinHash + LSH (RDD API).

The stages mirror the MapReduce jobs D1-D4 one to one:

  D1  shingle + MinHash + band keys  -> groupByKey(bucket) -> candidate pairs
  D2  distinct()                                         -> unique candidate pairs
  D3  join(candidate pairs, shingle sets) on the first id, then on the second
  D4  exact Jaccard >= threshold                          -> 'a<TAB>b<TAB>J'

The only intended difference from MapReduce is Spark's execution model:
intermediate data stays in the DAG instead of being written to HDFS between
jobs, and (with --cache) the RDD of shingle sets, which is used three times,
is persisted with StorageLevel.MEMORY_AND_DISK instead of being recomputed.
"""
import argparse
import json
import time

from pyspark import SparkConf, SparkContext, StorageLevel

from nddup import Params, load_params
from nddup.core import (MinHasher, band_keys, bucket_pairs, jaccard,
                        shingle_hashes, split_record)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, help="preprocessed reviews (TSV)")
    ap.add_argument("--output", required=True, help="similar pairs (TSV)")
    ap.add_argument("--params", default=None)
    ap.add_argument("--partitions", type=int, default=8, help="shuffle partitions (= MR reducers)")
    ap.add_argument("--cache", choices=["on", "off"], default="on")
    ap.add_argument("--save-candidates", default=None, help="also write candidate pairs (evaluation only)")
    ap.add_argument("--metrics", default=None)
    args = ap.parse_args()

    p = load_params(args.params)
    params_json = p.to_json()
    R = args.partitions
    sc = SparkContext(conf=SparkConf().setAppName("nddup-detect-cache-%s" % args.cache))
    sc.setLogLevel("WARN")
    t0 = time.time()

    acc = {name: sc.accumulator(0) for name in (
        "lsh_docs_signed", "lsh_nonsingleton_buckets", "lsh_pair_emissions",
        "lsh_oversized_buckets", "lsh_reviews_in_oversized_buckets",
        "candidate_pairs", "verified_pairs", "similar_pairs")}

    def local_params():
        return Params(**json.loads(params_json))

    # --- shingle sets: (rid, uint32 array) -----------------------------------
    def shingle_partition(lines):
        lp = local_params()
        for line in lines:
            rid, text = split_record(line)
            if text:
                yield rid, shingle_hashes(text, lp.shingle_k)

    shingled = sc.textFile(args.input).mapPartitions(shingle_partition)
    if args.cache == "on":
        shingled = shingled.persist(StorageLevel.MEMORY_AND_DISK)

    # --- D1: MinHash signatures -> band keys -> buckets -> pairs ---------------
    def band_partition(records):
        lp = local_params()
        mh = MinHasher(lp.num_hashes, lp.seed)
        n = 0
        for rid, sh in records:
            n += 1
            for key in band_keys(mh.signature(sh), lp.num_bands, lp.rows_per_band):
                yield key, rid
        acc["lsh_docs_signed"].add(n)

    def pair_partition(buckets):
        lp = local_params()
        c = dict.fromkeys(("nonsingleton", "emissions", "oversized", "oversized_reviews"), 0)
        for _key, rids in buckets:
            rids = list(rids)
            if len(rids) < 2:
                continue
            pairs = bucket_pairs(rids, lp.max_bucket)
            if pairs is None:
                c["oversized"] += 1
                c["oversized_reviews"] += len(rids)
                continue
            if pairs:
                c["nonsingleton"] += 1
                c["emissions"] += len(pairs)
            for pair in pairs:
                yield pair
        acc["lsh_nonsingleton_buckets"].add(c["nonsingleton"])
        acc["lsh_pair_emissions"].add(c["emissions"])
        acc["lsh_oversized_buckets"].add(c["oversized"])
        acc["lsh_reviews_in_oversized_buckets"].add(c["oversized_reviews"])

    pairs = (shingled.mapPartitions(band_partition)
             .groupByKey(numPartitions=R)
             .mapPartitions(pair_partition))

    # --- D2: unique candidate pairs ------------------------------------------
    candidates = pairs.distinct(numPartitions=R)

    def count_candidates(it):
        n = 0
        for pair in it:
            n += 1
            yield pair
        acc["candidate_pairs"].add(n)

    if args.save_candidates:
        candidates = candidates.persist(StorageLevel.MEMORY_AND_DISK)
        candidates.map(lambda ab: "%s\t%s" % ab).saveAsTextFile(args.save_candidates)
    candidates = candidates.mapPartitions(count_candidates)

    # --- D3: attach both shingle sets (two joins) --------------------------------
    with_a = candidates.join(shingled, numPartitions=R)                       # (a, (b, Sa))
    with_b = (with_a.map(lambda kv: (kv[1][0], (kv[0], kv[1][1])))
              .join(shingled, numPartitions=R))                               # (b, ((a, Sa), Sb))

    # --- D4: exact Jaccard verification ---------------------------------------
    def verify_partition(records):
        lp = local_params()
        verified = similar = 0
        for b, ((a, sa), sb) in records:
            verified += 1
            j = jaccard(sa, sb)
            if j >= lp.threshold:
                similar += 1
                yield "%s\t%s\t%.6f" % (a, b, j)
        acc["verified_pairs"].add(verified)
        acc["similar_pairs"].add(similar)

    with_b.mapPartitions(verify_partition).saveAsTextFile(args.output)

    metrics = {k: v.value for k, v in acc.items()}
    metrics.update({"driver_seconds": time.time() - t0, "cache": args.cache,
                    "partitions": R, "application_id": sc.applicationId})
    print("NDDUP_METRICS " + json.dumps(metrics))
    if args.metrics:
        with open(args.metrics, "w") as fh:
            json.dump(metrics, fh)
    if args.cache == "on":
        shingled.unpersist()
    sc.stop()


if __name__ == "__main__":
    main()
