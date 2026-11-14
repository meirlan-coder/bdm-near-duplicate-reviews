#!/usr/bin/env python3
"""Benchmark driver: MapReduce vs custom PySpark on the same data and cluster.

For every dataset and every repetition it runs preprocessing (job P) and
duplicate detection (jobs D*) of each framework, measures client-side
wall-clock times, collects the framework counters, checksums the output and
appends one JSON record per run to results/runs.jsonl.

Frameworks are interleaved (MR, Spark, Spark, MR, ...) so that a warm OS page
cache or a background hiccup does not systematically favour one of them.
Runs already present in runs.jsonl are skipped, so the script can be resumed.

    python3 bench/run_benchmark.py --datasets 1m,5m --runs 3 --warmup 1
"""
import argparse
import hashlib
import json
import os
import socket
import sys
import time
import traceback

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import pipelines as pl  # noqa: E402

RESULTS = os.path.join(pl.PROJECT, "results")
RUNS = os.path.join(RESULTS, "runs.jsonl")

VARIANTS = {
    "mr": ("mr", "n/a"),
    "spark": ("spark", "on"),
    "spark-nocache": ("spark", "off"),
}


def done_runs():
    done = set()
    if os.path.exists(RUNS):
        for line in open(RUNS):
            try:
                r = json.loads(line)
            except ValueError:
                continue
            if r.get("status") == "ok":
                done.add((r["dataset"], r["variant"], r["run_index"]))
    return done


def manifest():
    path = os.path.join(RESULTS, "data", "manifest.json")
    return json.load(open(path)) if os.path.exists(path) else {}


def run_one(ds, variant, idx, warmup, log):
    framework, cache = VARIANTS[variant]
    raw = "%s/raw/%s" % (pl.HDFS_ROOT, ds)
    work = "%s/work/%s/%s" % (pl.HDFS_ROOT, variant, ds)
    tag = "%s-run%d" % (ds, idx)
    stages = pl.run_pipeline(framework, raw, work, tag, cache=cache, log=log)
    lines = pl.hdfs_lines(work + "/similar")
    digest = hashlib.sha256("\n".join(lines).encode()).hexdigest()
    out_dir = os.path.join(RESULTS, "outputs")
    os.makedirs(out_dir, exist_ok=True)
    out_file = os.path.join(out_dir, "%s_%s.tsv" % (ds, variant))
    if not os.path.exists(out_file):
        with open(out_file, "w") as fh:
            fh.write("\n".join(lines) + ("\n" if lines else ""))
    m = manifest().get(ds, {})
    rec = {
        "status": "ok", "dataset": ds, "variant": variant, "framework": framework,
        "cache": cache, "run_index": idx, "warmup": warmup,
        "reviews": m.get("reviews"), "raw_bytes": pl.hdfs_du(raw),
        "prep_bytes": pl.hdfs_du(work + "/prep"),
        "prep_seconds": sum(j["seconds"] for j in stages["prep"]),
        "detect_seconds": sum(j["seconds"] for j in stages["detect"]),
        "prep_start": stages["prep"][0]["start"], "prep_end": stages["prep"][-1]["end"],
        "detect_start": stages["detect"][0]["start"], "detect_end": stages["detect"][-1]["end"],
        "counters": pl.summarize_counters(framework, stages),
        "jobs": stages["prep"] + stages["detect"],
        "output_pairs": len(lines), "output_sha256": digest,
        "partitions": pl.default_partitions(), "task_slots": pl.slots(),
        "client_host": socket.gethostname(), "finished_at": time.time(),
    }
    rec["total_seconds"] = rec["prep_seconds"] + rec["detect_seconds"]
    pl.hdfs_rm(work, log)          # free HDFS space between runs
    return rec


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--datasets", default="1m,5m")
    ap.add_argument("--variants", default="mr,spark")
    ap.add_argument("--runs", type=int, default=3)
    ap.add_argument("--warmup", type=int, default=1)
    args = ap.parse_args()

    pl.build_zip()
    os.makedirs(os.path.join(RESULTS, "logs"), exist_ok=True)
    variants = [v for v in args.variants.split(",") if v]
    for ds in [d for d in args.datasets.split(",") if d]:
        for i in range(args.warmup + args.runs):
            order = variants if i % 2 == 0 else list(reversed(variants))
            for variant in order:
                if (ds, variant, i) in done_runs():
                    continue
                log = os.path.join(RESULTS, "logs", "%s_%s_run%d.log" % (ds, variant, i))
                warm = i < args.warmup
                print("[bench] %s %-13s run %d%s ..." % (ds, variant, i, " (warm-up)" if warm else ""),
                      flush=True)
                for attempt in (1, 2):
                    try:
                        rec = run_one(ds, variant, i, warm, log)
                        break
                    except Exception as exc:  # keep going, record the failure
                        rec = {"status": "failed", "dataset": ds, "variant": variant,
                               "run_index": i, "attempt": attempt, "error": str(exc)[-2000:],
                               "trace": traceback.format_exc()[-2000:], "finished_at": time.time()}
                        print("[bench]   FAILED (attempt %d): %s" % (attempt, str(exc)[-300:]), flush=True)
                with open(RUNS, "a") as fh:
                    fh.write(json.dumps(rec) + "\n")
                if rec["status"] == "ok":
                    print("[bench]   prep %.1fs  detect %.1fs  candidates %d  similar pairs %d" % (
                        rec["prep_seconds"], rec["detect_seconds"],
                        rec["counters"]["candidate_pairs"], rec["output_pairs"]), flush=True)


if __name__ == "__main__":
    main()
