#!/usr/bin/env python3
"""MR job P (map-only): raw JSON review -> 'review_id<TAB>normalised text'."""
from nddup.core import preprocess_line
from streaming_util import Counters, params, stdin_lines, stdout


def main():
    p = params()
    c = Counters()
    out = stdout()
    for line in stdin_lines():
        c.inc("prep_input_reviews")
        rec = preprocess_line(line, p)
        if rec is None:
            c.inc("prep_dropped_reviews")
            continue
        c.inc("prep_output_reviews")
        out.write(rec + "\n")
    out.flush()
    c.flush()


if __name__ == "__main__":
    main()
