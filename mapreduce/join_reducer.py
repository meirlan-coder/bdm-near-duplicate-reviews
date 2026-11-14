#!/usr/bin/env python3
"""MR job D3 reduce: attach the review text to every pair the review is in.

For review x with text t and partners y1..yk emit, for each partner,
'min(x,y)<TAB>max(x,y)<TAB>t'.  Job D4 then sees both texts of every pair.
"""
from streaming_util import grouped, stdin_lines, stdout


def main():
    out = stdout()
    for (rid,), values in grouped(stdin_lines()):
        text, partners = None, []
        for v in values:
            tag, _, payload = v.partition("\t")
            if tag == "D":
                text = payload
            else:
                partners.append(payload)
        if text is None or not partners:
            continue
        for other in partners:
            a, b = (rid, other) if rid < other else (other, rid)
            out.write("%s\t%s\t%s\n" % (a, b, text))
    out.flush()


if __name__ == "__main__":
    main()
