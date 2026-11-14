#!/usr/bin/env python3
"""MR job D4 reduce: exact Jaccard verification of every candidate pair.

Run with stream.num.map.output.key.fields=2 (key = the pair).  Each key has
exactly two values: the texts of the two reviews.  Output 'a<TAB>b<TAB>J' for
pairs with J >= threshold.
"""
from nddup.core import jaccard, shingle_hashes
from streaming_util import Counters, grouped, params, stdin_lines, stdout


def main():
    p = params()
    c = Counters()
    out = stdout()
    for (a, b), texts in grouped(stdin_lines(), num_key_fields=2):
        if len(texts) != 2:
            c.inc("verify_incomplete_pairs")
            continue
        j = jaccard(shingle_hashes(texts[0], p.shingle_k), shingle_hashes(texts[1], p.shingle_k))
        c.inc("verified_pairs")
        if j >= p.threshold:
            c.inc("similar_pairs")
            out.write("%s\t%s\t%.6f\n" % (a, b, j))
    out.flush()
    c.flush()


if __name__ == "__main__":
    main()
