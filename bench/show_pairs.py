#!/usr/bin/env python3
"""Print a few detected near-duplicate pairs side by side (for the live demo).

    python3 bench/show_pairs.py --work /nddup/work/demo-spark --n 6
"""
import argparse
import os
import random
import sys
import textwrap

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import pipelines as pl  # noqa: E402


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--work", required=True, help="HDFS work dir of a finished run")
    ap.add_argument("--n", type=int, default=6)
    ap.add_argument("--seed", type=int, default=1)
    args = ap.parse_args()
    pairs = [l.split("\t") for l in pl.hdfs_lines(args.work + "/similar")]
    if not pairs:
        print("no similar pairs found")
        return
    rng = random.Random(args.seed)
    # prefer non-identical pairs: they are the interesting ones
    near = [p for p in pairs if float(p[2]) < 0.999]
    chosen = rng.sample(near, min(len(near), args.n - args.n // 3)) if near else []
    exact = [p for p in pairs if float(p[2]) >= 0.999]
    chosen += rng.sample(exact, min(len(exact), args.n - len(chosen)))
    need = {x for p in chosen for x in p[:2]}
    text = {}
    for line in pl.hdfs_lines(args.work + "/prep"):
        rid, _, t = line.partition("\t")
        if rid in need:
            text[rid] = t
    print("%d similar pairs in total (%d exact copies, %d near-duplicates)\n"
          % (len(pairs), len(exact), len(near)))
    w = textwrap.TextWrapper(width=100, initial_indent="    ", subsequent_indent="    ")
    for a, b, j in sorted(chosen, key=lambda p: float(p[2])):
        print("Jaccard = %.3f" % float(j))
        print("  %s:" % a)
        print(w.fill(text.get(a, "")[:350]))
        print("  %s:" % b)
        print(w.fill(text.get(b, "")[:350]))
        print("-" * 104)
    print("Note: textual similarity alone does not prove that a review is fake.")


if __name__ == "__main__":
    main()
