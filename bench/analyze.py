#!/usr/bin/env python3
"""Aggregate the benchmark runs into tables and figures.

Inputs : results/runs.jsonl, results/metrics/<container>.csv,
         results/eval/summary.json, results/data/manifest.json
Outputs: results/summary/summary.json, results/summary/tables.md,
         results/summary/runs.csv and results/summary/fig_*.png

Memory = anonymous memory (process memory, page cache excluded) sampled every
second from the cgroups of the containers; for every run and stage we take the
peak of the *sum over the worker containers* (where YARN runs the map/reduce
tasks and Spark executors) and, separately, the peak of the client container
(where the Spark driver / the MapReduce job client runs).
"""
import csv
import glob
import json
import math
import os
from collections import defaultdict

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

PROJECT = os.environ.get("PROJECT_DIR", os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
RES = os.path.join(PROJECT, "results")
OUT = os.path.join(RES, "summary")
COLORS = {"mr": "#C2551F", "spark": "#2A6FB0", "spark-nocache": "#7FA8D6"}
LABELS = {"mr": "Hadoop MapReduce", "spark": "PySpark (cache on)", "spark-nocache": "PySpark (cache off)"}
GB = 2.0 ** 30


def mean_std(xs):
    xs = [x for x in xs if x is not None]
    if not xs:
        return None, None
    m = sum(xs) / len(xs)
    sd = math.sqrt(sum((x - m) ** 2 for x in xs) / (len(xs) - 1)) if len(xs) > 1 else 0.0
    return m, sd


def load_runs():
    runs = []
    path = os.path.join(RES, "runs.jsonl")
    if os.path.exists(path):
        for line in open(path):
            try:
                r = json.loads(line)
            except ValueError:
                continue
            if r.get("status") == "ok":
                runs.append(r)
    return runs


def load_metrics():
    series = {}
    for path in glob.glob(os.path.join(RES, "metrics", "*.csv")):
        host = os.path.basename(path)[:-4]
        rows = []
        with open(path) as fh:
            for row in csv.DictReader(fh):
                try:
                    rows.append((float(row["ts"]), int(row["anon_bytes"]), int(row["mem_bytes"]),
                                 int(row["cpu_usec"])))
                except (ValueError, KeyError, TypeError):
                    continue
        rows.sort()
        series[host] = rows
    return series


def window(rows, start, end):
    return [r for r in rows if start <= r[0] <= end]


def resources(series, start, end):
    """Peak/mean worker memory, peak client memory, avg CPU cores used by workers."""
    by_sec = defaultdict(int)
    cpu_cores = 0.0
    workers = [h for h in series if h.startswith("worker")]
    for h in workers:
        w = window(series[h], start, end)
        for ts, anon, _mem, _cpu in w:
            by_sec[int(ts)] += anon
        if len(w) >= 2:
            cpu_cores += (w[-1][3] - w[0][3]) / 1e6 / max(1e-9, w[-1][0] - w[0][0])
    client = window(series.get("client", []), start, end)
    out = {
        "worker_mem_peak_gb": max(by_sec.values()) / GB if by_sec else None,
        "worker_mem_mean_gb": (sum(by_sec.values()) / len(by_sec) / GB) if by_sec else None,
        "client_mem_peak_gb": max(r[1] for r in client) / GB if client else None,
        "worker_cpu_cores_avg": cpu_cores if workers else None,
    }
    return out


def main():
    os.makedirs(OUT, exist_ok=True)
    runs = load_runs()
    series = load_metrics()
    manifest = json.load(open(os.path.join(RES, "data", "manifest.json"))) \
        if os.path.exists(os.path.join(RES, "data", "manifest.json")) else {}
    measured = [r for r in runs if not r.get("warmup")]

    flat = []
    for r in measured:
        row = {"dataset": r["dataset"], "variant": r["variant"], "run": r["run_index"],
               "reviews": r.get("reviews"), "raw_mb": (r.get("raw_bytes") or 0) / 2 ** 20,
               "prep_mb": (r.get("prep_bytes") or 0) / 2 ** 20,
               "prep_s": r["prep_seconds"], "detect_s": r["detect_seconds"], "total_s": r["total_seconds"],
               "reviews_after_prep": r["counters"]["prep_output_reviews"],
               "candidate_pairs": r["counters"]["candidate_pairs"],
               "pair_emissions": r["counters"]["lsh_pair_emissions"],
               "oversized_buckets": r["counters"]["lsh_oversized_buckets"],
               "similar_pairs": r["output_pairs"], "output_sha256": r["output_sha256"]}
        for stage in ("prep", "detect"):
            res = resources(series, r[stage + "_start"], r[stage + "_end"])
            for k, v in res.items():
                row["%s_%s" % (stage, k)] = v
        for j in r["jobs"]:
            row["job_%s_s" % j["stage_job"]] = j["seconds"]
            c = j.get("counters", {})
            for k in ("Peak Map Physical memory (bytes)", "Peak Reduce Physical memory (bytes)"):
                if k in c:
                    key = "job_%s_%s_gb" % (j["stage_job"], "peak_map_task_mem" if "Map" in k else "peak_reduce_task_mem")
                    row[key] = c[k] / GB
        flat.append(row)

    # ---- per-run CSV ------------------------------------------------------
    if flat:
        keys = sorted({k for row in flat for k in row})
        with open(os.path.join(OUT, "runs.csv"), "w", newline="") as fh:
            w = csv.DictWriter(fh, fieldnames=keys)
            w.writeheader()
            for row in flat:
                w.writerow(row)

    # ---- aggregate -----------------------------------------------------------
    groups = defaultdict(list)
    for row in flat:
        groups[(row["dataset"], row["variant"])].append(row)
    agg = {}
    for (ds, var), rows in sorted(groups.items()):
        a = {"runs": len(rows), "reviews": rows[0]["reviews"], "raw_mb": rows[0]["raw_mb"],
             "prep_mb": rows[0]["prep_mb"], "reviews_after_prep": rows[0]["reviews_after_prep"]}
        numeric = sorted({k for row in rows for k, v in row.items()
                          if isinstance(v, (int, float)) and k not in ("run",)})
        for k in numeric:
            m, sd = mean_std([row.get(k) for row in rows])
            a[k] = m
            a[k + "_std"] = sd
        a["prep_throughput_reviews_s"] = a["reviews"] / a["prep_s"] if a.get("reviews") else None
        a["prep_throughput_mb_s"] = a["raw_mb"] / a["prep_s"] if a.get("raw_mb") else None
        a["detect_throughput_reviews_s"] = a["reviews_after_prep"] / a["detect_s"] if a.get("detect_s") else None
        a["total_throughput_reviews_s"] = a["reviews"] / a["total_s"] if a.get("reviews") else None
        a["outputs_consistent_across_runs"] = len({row["output_sha256"] for row in rows}) == 1
        a["output_sha256"] = rows[0]["output_sha256"]
        agg["%s/%s" % (ds, var)] = a

    datasets = sorted({ds for ds, _ in groups}, key=lambda d: manifest.get(d, {}).get("reviews", 0))
    variants = [v for v in ("mr", "spark", "spark-nocache") if any(var == v for _, var in groups)]
    comparisons = {}
    for ds in datasets:
        mr, sp = agg.get(ds + "/mr"), agg.get(ds + "/spark")
        if mr and sp:
            comparisons[ds] = {
                "identical_output_mr_vs_spark": mr["output_sha256"] == sp["output_sha256"],
                "speedup_prep": mr["prep_s"] / sp["prep_s"],
                "speedup_detect": mr["detect_s"] / sp["detect_s"],
                "speedup_total": mr["total_s"] / sp["total_s"],
            }
    scaling = {}
    if len(datasets) >= 2:
        small, large = datasets[0], datasets[-1]
        for v in variants:
            s, l = agg.get(small + "/" + v), agg.get(large + "/" + v)
            if s and l and s.get("reviews"):
                scaling[v] = {"from": small, "to": large, "data_ratio": l["reviews"] / s["reviews"],
                              "prep_time_ratio": l["prep_s"] / s["prep_s"],
                              "detect_time_ratio": l["detect_s"] / s["detect_s"],
                              "candidate_ratio": (l["candidate_pairs"] / s["candidate_pairs"])
                              if s["candidate_pairs"] else None}

    env = {}
    env_file = os.path.join(RES, "env", "env.json")
    if os.path.exists(env_file):
        env = json.load(open(env_file))
    evaluation = json.load(open(os.path.join(RES, "eval", "summary.json"))) \
        if os.path.exists(os.path.join(RES, "eval", "summary.json")) else None
    summary = {"aggregate": agg, "comparisons": comparisons, "scaling": scaling,
               "datasets": {d: manifest.get(d) for d in manifest}, "environment": env,
               "evaluation": {k: v for k, v in (evaluation or {}).items() if k != "examples"}}
    with open(os.path.join(OUT, "summary.json"), "w") as fh:
        json.dump(summary, fh, indent=2)

    # ---- markdown tables ------------------------------------------------------
    def f(x, nd=1):
        return "-" if x is None else ("%.*f" % (nd, x))

    md = ["# Benchmark summary", ""]
    md.append("| dataset | framework | reviews | raw MB | prep s | detect s | total s | prep rev/s | detect rev/s | "
              "peak worker mem GB (prep / detect) | peak client mem GB (detect) | candidate pairs | similar pairs |")
    md.append("|" + "---|" * 13)
    for ds in datasets:
        for v in variants:
            a = agg.get(ds + "/" + v)
            if not a:
                continue
            md.append("| %s | %s | %s | %s | %s ± %s | %s ± %s | %s ± %s | %s | %s | %s / %s | %s | %s | %s |" % (
                ds, LABELS[v], "{:,}".format(int(a["reviews"] or 0)), f(a["raw_mb"], 0),
                f(a["prep_s"]), f(a["prep_s_std"]), f(a["detect_s"]), f(a["detect_s_std"]),
                f(a["total_s"]), f(a["total_s_std"]), f(a["prep_throughput_reviews_s"], 0),
                f(a["detect_throughput_reviews_s"], 0), f(a.get("prep_worker_mem_peak_gb"), 2),
                f(a.get("detect_worker_mem_peak_gb"), 2), f(a.get("detect_client_mem_peak_gb"), 2),
                "{:,}".format(int(a["candidate_pairs"])), "{:,}".format(int(a["similar_pairs"]))))
    md += ["", "## MR vs Spark", ""]
    for ds, c in comparisons.items():
        md.append("* %s: speed-up Spark/MR prep %.2fx, detect %.2fx, total %.2fx; identical output: %s"
                  % (ds, c["speedup_prep"], c["speedup_detect"], c["speedup_total"],
                     c["identical_output_mr_vs_spark"]))
    md += ["", "## MapReduce job breakdown (mean seconds)", "", "| dataset | P | D1 | D2 | D3 | D4 |", "|---|---|---|---|---|---|"]
    for ds in datasets:
        a = agg.get(ds + "/mr")
        if a:
            md.append("| %s | %s |" % (ds, " | ".join(f(a.get("job_%s_s" % j)) for j in ("P", "D1", "D2", "D3", "D4"))))
    with open(os.path.join(OUT, "tables.md"), "w") as fh:
        fh.write("\n".join(md) + "\n")

    # ---- figures ------------------------------------------------------------------
    plt.rcParams.update({"font.size": 10, "axes.spines.top": False, "axes.spines.right": False})
    if datasets and variants:
        fig, axes = plt.subplots(1, 2, figsize=(10, 3.8))
        for ax, stage, title in zip(axes, ("prep_s", "detect_s"), ("Preprocessing", "Duplicate detection")):
            width = 0.8 / len(variants)
            for i, v in enumerate(variants):
                xs, ys, es = [], [], []
                for k, ds in enumerate(datasets):
                    a = agg.get(ds + "/" + v)
                    if a:
                        xs.append(k + (i - (len(variants) - 1) / 2) * width)
                        ys.append(a[stage])
                        es.append(a[stage + "_std"] or 0)
                ax.bar(xs, ys, width, yerr=es, capsize=3, color=COLORS[v], label=LABELS[v])
            ax.set_xticks(range(len(datasets)))
            ax.set_xticklabels(["%s\n(%s reviews)" % (d, "{:,}".format(manifest.get(d, {}).get("reviews", 0)))
                                for d in datasets])
            ax.set_ylabel("wall-clock time [s]")
            ax.set_title(title)
        axes[0].legend(frameon=False)
        fig.tight_layout()
        fig.savefig(os.path.join(OUT, "fig_times.png"), dpi=160)
        plt.close(fig)

        fig, ax = plt.subplots(figsize=(5.5, 3.8))
        for v in variants:
            pts = [(agg[ds + "/" + v]["reviews"], agg[ds + "/" + v]["detect_s"]) for ds in datasets
                   if ds + "/" + v in agg]
            ax.plot([p[0] / 1e6 for p in pts], [p[1] for p in pts], "o-", color=COLORS[v], label=LABELS[v])
        ax.set_xlabel("reviews [millions]")
        ax.set_ylabel("detection time [s]")
        ax.set_title("Scaling of duplicate detection")
        ax.legend(frameon=False)
        fig.tight_layout()
        fig.savefig(os.path.join(OUT, "fig_scaling.png"), dpi=160)
        plt.close(fig)

        fig, ax = plt.subplots(figsize=(6.5, 3.8))
        width = 0.8 / len(variants)
        for i, v in enumerate(variants):
            xs, ys = [], []
            for k, ds in enumerate(datasets):
                a = agg.get(ds + "/" + v)
                if a and a.get("detect_worker_mem_peak_gb") is not None:
                    xs.append(k + (i - (len(variants) - 1) / 2) * width)
                    ys.append(a["detect_worker_mem_peak_gb"])
            ax.bar(xs, ys, width, color=COLORS[v], label=LABELS[v])
        ax.set_xticks(range(len(datasets)))
        ax.set_xticklabels(datasets)
        ax.set_ylabel("peak memory of worker containers [GB]")
        ax.set_title("Memory during duplicate detection")
        ax.legend(frameon=False)
        fig.tight_layout()
        fig.savefig(os.path.join(OUT, "fig_memory.png"), dpi=160)
        plt.close(fig)

        # memory/CPU timeline of the first measured run of the largest dataset
        big = datasets[-1]
        fig, ax = plt.subplots(figsize=(10, 3.8))
        for v in variants:
            cand = [r for r in measured if r["dataset"] == big and r["variant"] == v]
            if not cand:
                continue
            r = cand[0]
            t0 = r["prep_start"]
            by_sec = defaultdict(int)
            for h in series:
                if h.startswith("worker"):
                    for ts, anon, _m, _c in window(series[h], r["prep_start"], r["detect_end"]):
                        by_sec[int(ts)] += anon
            xs = sorted(by_sec)
            ax.plot([x - t0 for x in xs], [by_sec[x] / GB for x in xs], color=COLORS[v], label=LABELS[v])
            ax.axvline(r["detect_start"] - t0, color=COLORS[v], ls=":", lw=1)
        ax.set_xlabel("seconds since start of preprocessing (dotted line: detection starts)")
        ax.set_ylabel("worker memory [GB]")
        ax.set_title("Worker memory over one run (%s)" % big)
        ax.legend(frameon=False)
        fig.tight_layout()
        fig.savefig(os.path.join(OUT, "fig_timeline.png"), dpi=160)
        plt.close(fig)

        a = [agg.get(ds + "/mr") for ds in datasets]
        if any(a):
            fig, ax = plt.subplots(figsize=(6.5, 3.8))
            bottom = [0.0] * len(datasets)
            palette = ["#8C8C8C", "#C2551F", "#E08A4F", "#F0B98C", "#F7DCC4"]
            for j, col in zip(("P", "D1", "D2", "D3", "D4"), palette):
                vals = [(x or {}).get("job_%s_s" % j) or 0 for x in a]
                ax.bar(range(len(datasets)), vals, bottom=bottom, color=col, label=j)
                bottom = [b + v for b, v in zip(bottom, vals)]
            ax.set_xticks(range(len(datasets)))
            ax.set_xticklabels(datasets)
            ax.set_ylabel("seconds")
            ax.set_title("MapReduce: time per job")
            ax.legend(frameon=False, ncol=5)
            fig.tight_layout()
            fig.savefig(os.path.join(OUT, "fig_mr_jobs.png"), dpi=160)
            plt.close(fig)

    if evaluation:
        curve = [c for c in evaluation["s_curve"] if c["empirical_probability"] is not None]
        p = evaluation["params"]
        b, r = p["num_bands"], p["rows_per_band"]
        fig, ax = plt.subplots(figsize=(5.5, 3.8))
        xs = [i / 100 for i in range(0, 101)]
        ax.plot(xs, [1 - (1 - x ** r) ** b for x in xs], color="#555555", label="theory 1-(1-s^r)^b")
        mids = []
        for c in curve:
            lo, hi = [float(t) for t in c["bin"].strip("[])").split(",")]
            mids.append((lo + hi) / 2)
        ax.plot(mids, [c["empirical_probability"] for c in curve], "o", color=COLORS["mr"],
                label="measured on sample")
        ax.axvline(p["threshold"], ls="--", color="#999999", lw=1)
        ax.set_xlabel("Jaccard similarity of the pair")
        ax.set_ylabel("P(pair becomes a candidate)")
        ax.set_title("LSH S-curve (b=%d, r=%d)" % (b, r))
        ax.legend(frameon=False, loc="lower right")
        fig.tight_layout()
        fig.savefig(os.path.join(OUT, "fig_s_curve.png"), dpi=160)
        plt.close(fig)

    print(open(os.path.join(OUT, "tables.md")).read())
    print(json.dumps(comparisons, indent=2))


if __name__ == "__main__":
    main()
