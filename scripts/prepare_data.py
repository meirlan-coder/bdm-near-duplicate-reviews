#!/usr/bin/env python3
"""Download Amazon Reviews 2023 and build the benchmark datasets on HDFS.

Datasets (all nested, all drawn with a fixed seed):
  5m      uniform random sample of 5,000,000 reviews of one category
  1m      uniform random sample of 1,000,000 reviews of the 5m dataset
  sample  all reviews of randomly chosen *users* of the 1m dataset, until
          --sample-size reviews are collected (evaluation with exact all-pairs;
          sampling whole users keeps the pairs a user wrote, which is where most
          near-duplicates come from, so the sample contains enough similar pairs)
  smoke   20,000 synthetic reviews with planted near-duplicates (smoke test)

Every review gets a stable id 'R' + 9-digit line number in the original file.

    python3 scripts/prepare_data.py --category Baby_Products --sizes 1m,5m
"""
import argparse
import json
import os
import subprocess
import sys

import numpy as np

PROJECT = os.environ.get("PROJECT_DIR", "/opt/project")
LOCAL = os.environ.get("LOCAL_DATA", "/localdata")
URL = ("https://huggingface.co/datasets/McAuley-Lab/Amazon-Reviews-2023/"
       "resolve/main/raw/review_categories/%s.jsonl")
SIZES = {"1m": 1_000_000, "5m": 5_000_000, "10m": 10_000_000, "20m": 20_000_000}
MANIFEST = os.path.join(PROJECT, "results", "data", "manifest.json")


def run(cmd, check=True):
    print("$ " + " ".join(cmd), flush=True)
    return subprocess.run(cmd, check=check)


def load_manifest():
    return json.load(open(MANIFEST)) if os.path.exists(MANIFEST) else {}


def save_manifest(m):
    os.makedirs(os.path.dirname(MANIFEST), exist_ok=True)
    json.dump(m, open(MANIFEST, "w"), indent=2, sort_keys=True)


def download(category):
    os.makedirs(os.path.join(LOCAL, "raw"), exist_ok=True)
    path = os.path.join(LOCAL, "raw", category + ".jsonl")
    done = path + ".complete"
    if not os.path.exists(done):
        run(["curl", "-fL", "--retry", "10", "--retry-delay", "5", "-C", "-",
             "-o", path, URL % category])
        open(done, "w").close()
    return path


def count_lines(path):
    out = subprocess.run(["wc", "-l", path], stdout=subprocess.PIPE, check=True,
                         universal_newlines=True).stdout
    return int(out.split()[0])


def with_id(line, idx):
    line = line.strip()
    return '{"review_id": "R%09d", %s' % (idx, line[1:]) if line.startswith("{") else None


def write_subsets(src, total, sizes, seed):
    """One pass over the source file writes all requested nested subsets."""
    rng = np.random.RandomState(seed)
    order = sorted(sizes, key=lambda s: -SIZES[s])
    masks, prev = {}, np.arange(total)
    for name in order:
        n = SIZES[name]
        if n > len(prev):
            sys.exit("dataset %s needs %d reviews but only %d are available; "
                     "choose a bigger --category (e.g. Beauty_and_Personal_Care has ~24M)"
                     % (name, n, len(prev)))
        chosen = np.sort(rng.choice(prev, size=n, replace=False))
        mask = np.zeros(total, dtype=bool)
        mask[chosen] = True
        masks[name], prev = mask, chosen
    outs = {name: open(os.path.join(LOCAL, "%s.jsonl" % name), "w", encoding="utf-8") for name in order}
    with open(src, encoding="utf-8") as fh:
        for idx, line in enumerate(fh):
            if idx >= total:
                break
            rec = None
            for name in order:
                if masks[name][idx]:
                    rec = rec or with_id(line, idx)
                    if rec:
                        outs[name].write(rec + "\n")
    for f in outs.values():
        f.close()


def user_sample(src, target, seed):
    users = {}
    with open(src, encoding="utf-8") as fh:
        for line in fh:
            users.setdefault(json.loads(line).get("user_id"), []).append(line)
    keys = sorted(k for k in users if k)
    rng = np.random.RandomState(seed + 1)
    rng.shuffle(keys)
    out, n_users = [], 0
    for k in keys:
        if len(out) >= target:
            break
        out.extend(users[k])
        n_users += 1
    path = os.path.join(LOCAL, "sample.jsonl")
    with open(path, "w", encoding="utf-8") as fh:
        fh.writelines(out)
    return path, n_users


def put_hdfs(local, name):
    target = "/nddup/raw/%s" % name
    run(["hdfs", "dfs", "-rm", "-r", "-f", "-skipTrash", target], check=False)
    run(["hdfs", "dfs", "-mkdir", "-p", target])
    run(["hdfs", "dfs", "-put", "-f", local, target + "/reviews.jsonl"])


def register(m, name, local, **extra):
    reviews = count_lines(local)
    m[name] = dict(reviews=reviews, bytes=os.path.getsize(local), **extra)
    save_manifest(m)
    print("[data] %-6s %10d reviews  %8.1f MB" % (name, reviews, os.path.getsize(local) / 2 ** 20), flush=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--category", default="Baby_Products")
    ap.add_argument("--sizes", default="1m,5m")
    ap.add_argument("--sample-size", type=int, default=40000)
    ap.add_argument("--seed", type=int, default=2026)
    ap.add_argument("--smoke-only", action="store_true")
    args = ap.parse_args()
    m = load_manifest()

    if "smoke" not in m:
        smoke = os.path.join(LOCAL, "smoke.jsonl")
        os.makedirs(LOCAL, exist_ok=True)
        run([sys.executable, os.path.join(PROJECT, "scripts", "make_synthetic.py"),
             "--n", "20000", "--out", smoke])
        put_hdfs(smoke, "smoke")
        register(m, "smoke", smoke, source="synthetic (scripts/make_synthetic.py)")
    if args.smoke_only:
        return

    sizes = [s for s in args.sizes.split(",") if s]
    if "1m" not in sizes:
        sizes.append("1m")              # the evaluation sample is drawn from 1m
    missing = [s for s in sizes + ["sample"] if s not in m]
    if not missing:
        print("[data] all datasets already prepared", flush=True)
        return
    src = download(args.category)
    total = count_lines(src)
    print("[data] %s: %d reviews in the source file" % (args.category, total), flush=True)
    todo = [s for s in sizes if s not in m]
    if todo:
        write_subsets(src, total, todo, args.seed)
        for s in sorted(todo, key=lambda s: SIZES[s]):
            local = os.path.join(LOCAL, "%s.jsonl" % s)
            put_hdfs(local, s)
            register(m, s, local, source="Amazon Reviews 2023 / %s" % args.category,
                     source_reviews=total, selection="uniform random sample, seed %d" % args.seed)
    if "sample" not in m:
        path, n_users = user_sample(os.path.join(LOCAL, "1m.jsonl"), args.sample_size, args.seed)
        put_hdfs(path, "sample")
        register(m, "sample", path, source="1m dataset", users=n_users,
                 selection="all reviews of %d random users (seed %d)" % (n_users, args.seed + 1))


if __name__ == "__main__":
    main()
