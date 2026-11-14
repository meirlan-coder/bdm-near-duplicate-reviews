#!/usr/bin/env python3
"""Where does the time go?  MapReduce jobs vs Spark stages (measured runs).

MapReduce : per-job wall time + framework counters (CPU time, shuffle bytes,
            spilled records) from results/runs.jsonl.
Spark     : per-stage duration, executor run time, shuffle write and disk
            spill from the Spark event logs (results/spark-events), matched to
            the runs through the application id.

Writes results/summary/stage_breakdown.json and fig_stages.png.
"""
import json
import os
from collections import defaultdict

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

PROJECT = os.environ.get("PROJECT_DIR", os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
RES = os.path.join(PROJECT, "results")
MB = 2.0 ** 20

SPARK_STAGE_LABELS = {
    0: "S0 read + shingle + MinHash + band keys (shuffle write)",
    1: "S1 group buckets -> pairs, distinct (map side)",
    2: "S2 read shingle sets for join (map side)",
    3: "S3 distinct + 1st join",
    4: "S4 2nd join + Jaccard + write",
}
MR_LABELS = {"D1": "D1 LSH buckets -> pairs", "D2": "D2 unique pairs",
             "D3": "D3 join texts", "D4": "D4 Jaccard verify"}


def mean(xs):
    return sum(xs) / len(xs) if xs else None


def spark_stages(app_file):
    stages = {}
    for line in open(app_file):
        e = json.loads(line)
        if e.get("Event") != "SparkListenerStageCompleted":
            continue
        si = e["Stage Info"]
        acc = {a["Name"]: float(a.get("Value") or 0) for a in si.get("Accumulables", [])
               if isinstance(a.get("Value"), (int, float, str)) and str(a.get("Value")).replace(".", "", 1).isdigit()}
        stages[si["Stage ID"]] = {
            "name": si["Stage Name"].split(" at ")[0],
            "seconds": (si["Completion Time"] - si["Submission Time"]) / 1000.0,
            "tasks": si["Number of Tasks"],
            "executor_run_s": acc.get("internal.metrics.executorRunTime", 0) / 1000.0,
            "shuffle_write_mb": acc.get("internal.metrics.shuffle.write.bytesWritten", 0) / MB,
            "disk_spill_mb": acc.get("internal.metrics.diskBytesSpilled", 0) / MB,
            "gc_s": acc.get("internal.metrics.jvmGCTime", 0) / 1000.0,
        }
    return stages


def main():
    runs = [json.loads(l) for l in open(os.path.join(RES, "runs.jsonl")) if l.strip()]
    runs = [r for r in runs if r.get("status") == "ok" and not r.get("warmup")]
    out = {}
    for ds in sorted({r["dataset"] for r in runs}):
        mr = [r for r in runs if r["dataset"] == ds and r["variant"] == "mr"]
        sp = [r for r in runs if r["dataset"] == ds and r["variant"] == "spark"]
        mr_jobs = defaultdict(lambda: defaultdict(list))
        for r in mr:
            for j in r["jobs"]:
                c = j["counters"]
                d = mr_jobs[j["stage_job"]]
                d["seconds"].append(j["seconds"])
                d["cpu_s"].append(c.get("CPU time spent (ms)", 0) / 1000.0)
                d["shuffle_mb"].append(c.get("Reduce shuffle bytes", 0) / MB)
                d["map_output_records"].append(c.get("Map output records", 0))
                d["spilled_records"].append(c.get("Spilled Records", 0))
        sp_stages = defaultdict(lambda: defaultdict(list))
        for r in sp:
            app = [j["job_id"] for j in r["jobs"] if j["stage_job"] == "D"][0]
            path = os.path.join(RES, "spark-events", app)
            if not os.path.exists(path):
                continue
            for sid, st in spark_stages(path).items():
                for k, v in st.items():
                    if k != "name":
                        sp_stages[sid][k].append(v)
        out[ds] = {
            "mapreduce_jobs": {j: {k: mean(v) for k, v in d.items()} for j, d in sorted(mr_jobs.items())},
            "spark_detect_stages": {str(s): dict({k: mean(v) for k, v in d.items()},
                                                 label=SPARK_STAGE_LABELS.get(s, "S%d" % s))
                                    for s, d in sorted(sp_stages.items())},
        }
    with open(os.path.join(RES, "summary", "stage_breakdown.json"), "w") as fh:
        json.dump(out, fh, indent=2)

    dsets = sorted(out)
    fig, axes = plt.subplots(1, len(dsets), figsize=(5.2 * len(dsets), 3.6), squeeze=False)
    mr_cols = ["#C2551F", "#D9773F", "#E89A63", "#F2C29C"]
    sp_cols = ["#1F4E80", "#2A6FB0", "#4F8CC7", "#7FA8D6", "#B3CCE8"]
    for ax, ds in zip(axes[0], dsets):
        left = 0.0
        for col, (j, label) in zip(mr_cols, MR_LABELS.items()):
            v = out[ds]["mapreduce_jobs"].get(j, {}).get("seconds") or 0
            ax.barh(1, v, left=left, color=col, edgecolor="white")
            if v > 0.06 * ax.get_xlim()[1] or v > 40:
                ax.text(left + v / 2, 1, j, ha="center", va="center", color="white", fontsize=8)
            left += v
        left = 0.0
        for col, (s, st) in zip(sp_cols, out[ds]["spark_detect_stages"].items()):
            v = st["seconds"]
            ax.barh(0, v, left=left, color=col, edgecolor="white")
            if v > 40:
                ax.text(left + v / 2, 0, "S" + s, ha="center", va="center", color="white", fontsize=8)
            left += v
        ax.set_yticks([0, 1])
        ax.set_yticklabels(["PySpark", "MapReduce"])
        ax.set_xlabel("seconds (detection only, mean of 3 runs)")
        ax.set_title(ds)
        for side in ("top", "right"):
            ax.spines[side].set_visible(False)
    fig.tight_layout()
    fig.savefig(os.path.join(RES, "summary", "fig_stages.png"), dpi=160)
    print(json.dumps(out, indent=1))


if __name__ == "__main__":
    main()
