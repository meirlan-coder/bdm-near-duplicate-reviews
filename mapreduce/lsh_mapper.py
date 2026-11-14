#!/usr/bin/env python3
"""MR job D1 map: review -> one (bucket_key, review_id) record per LSH band.

shingling -> MinHash signature -> b band keys.
"""
from nddup.core import MinHasher, band_keys, shingle_hashes, split_record
from streaming_util import Counters, params, stdin_lines, stdout


def main():
    p = params()
    mh = MinHasher(p.num_hashes, p.seed)
    c = Counters()
    out = stdout()
    for line in stdin_lines():
        rid, text = split_record(line)
        if not text:
            continue
        sig = mh.signature(shingle_hashes(text, p.shingle_k))
        for key in band_keys(sig, p.num_bands, p.rows_per_band):
            out.write("%s\t%s\n" % (key, rid))
        c.inc("lsh_docs_signed")
    out.flush()
    c.flush()


if __name__ == "__main__":
    main()
