#!/usr/bin/env python3
"""MR job D1 reduce: LSH bucket -> all candidate pairs 'a<TAB>b' (a < b).

The same pair may be produced by several bands; job D2 removes duplicates.
"""
from nddup.core import bucket_pairs
from streaming_util import Counters, grouped, params, stdin_lines, stdout


def main():
    p = params()
    c = Counters()
    out = stdout()
    for _key, rids in grouped(stdin_lines()):
        if len(rids) < 2:
            continue
        pairs = bucket_pairs(rids, p.max_bucket)
        if pairs is None:
            c.inc("lsh_oversized_buckets")
            c.inc("lsh_reviews_in_oversized_buckets", len(rids))
            continue
        if pairs:
            c.inc("lsh_nonsingleton_buckets")
            c.inc("lsh_pair_emissions", len(pairs))
        for a, b in pairs:
            out.write("%s\t%s\n" % (a, b))
    out.flush()
    c.flush()


if __name__ == "__main__":
    main()
