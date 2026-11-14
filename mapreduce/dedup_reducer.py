#!/usr/bin/env python3
"""MR job D2 reduce: keep each candidate pair once.

Run with stream.num.map.output.key.fields=2, so the whole pair is the key and
identical pairs arrive consecutively.
"""
from streaming_util import Counters, grouped, stdin_lines, stdout


def main():
    c = Counters()
    out = stdout()
    for (a, b), _ in grouped(stdin_lines(), num_key_fields=2):
        c.inc("candidate_pairs")
        out.write("%s\t%s\n" % (a, b))
    out.flush()
    c.flush()


if __name__ == "__main__":
    main()
