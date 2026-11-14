#!/usr/bin/env python3
"""MR job D3 map (reduce-side join, step 1).

Input is the union of
  * preprocessed reviews  'rid<TAB>text'   -> emit 'rid<TAB>D<TAB>text'
  * candidate pairs       'a<TAB>b'        -> emit 'a<TAB>P<TAB>b' and 'b<TAB>P<TAB>a'
A candidate-pair line is recognised because both fields are review ids
(upper-case 'R' + digits); normalised review text is always lower-case.
"""
from streaming_util import stdin_lines, stdout


def is_rid(s):
    return len(s) > 1 and s[0] == "R" and s[1:].isdigit()


def main():
    out = stdout()
    for line in stdin_lines():
        first, _, second = line.partition("\t")
        if is_rid(second):
            out.write("%s\tP\t%s\n" % (first, second))
            out.write("%s\tP\t%s\n" % (second, first))
        elif second:
            out.write("%s\tD\t%s\n" % (first, second))
    out.flush()


if __name__ == "__main__":
    main()
