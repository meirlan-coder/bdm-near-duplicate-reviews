#!/usr/bin/env python3
"""Run the Hadoop Streaming pipeline *without* Hadoop (for tests / debugging).

It reproduces the streaming contract with plain processes: every input file is
fed to a separate mapper process, mapper output is hash-partitioned over R
reducers, each partition is sorted by key (byte order, like Hadoop's Text
comparator) and fed to a reducer process.  The real benchmark always runs on
the YARN cluster (bench/pipelines.py); this runner only checks correctness.

    python3 mapreduce/local_runner.py --input data/synthetic.jsonl --work /tmp/mrwork
"""
import argparse
import glob
import os
import re
import subprocess
import sys
import zlib

HERE = os.path.dirname(os.path.abspath(__file__))
SRC = os.path.join(HERE, "..", "src")
COUNTER_RE = re.compile(r"^reporter:counter:([^,]+),([^,]+),(\d+)$")


def _env():
    env = dict(os.environ)
    env["PYTHONPATH"] = os.pathsep.join([SRC, HERE, env.get("PYTHONPATH", "")])
    return env


def _run(cmd, data, counters):
    if cmd == "cat":
        return data
    proc = subprocess.run([sys.executable, os.path.join(HERE, cmd)], input=data,
                          capture_output=True, env=_env())
    if proc.returncode != 0:
        raise RuntimeError("%s failed:\n%s" % (cmd, proc.stderr.decode()))
    for line in proc.stderr.decode().splitlines():
        m = COUNTER_RE.match(line)
        if m:
            counters[m.group(2)] = counters.get(m.group(2), 0) + int(m.group(3))
    return proc.stdout


def run_job(name, inputs, output_dir, mapper, reducer=None, reduces=4, key_fields=1):
    counters = {}
    map_out = [_run(mapper, open(f, "rb").read(), counters) for f in inputs]
    os.makedirs(output_dir, exist_ok=True)
    for f in glob.glob(os.path.join(output_dir, "part-*")):
        os.remove(f)
    if reducer is None:
        for i, data in enumerate(map_out):
            open(os.path.join(output_dir, "part-%05d" % i), "wb").write(data)
        return counters
    parts = [[] for _ in range(reduces)]
    for data in map_out:
        for line in data.splitlines():
            key = b"\t".join(line.split(b"\t", key_fields)[:key_fields])
            parts[zlib.crc32(key) % reduces].append(line)
    for i, lines in enumerate(parts):
        lines.sort(key=lambda l: l.split(b"\t", key_fields)[:key_fields])
        data = b"".join(l + b"\n" for l in lines)
        open(os.path.join(output_dir, "part-%05d" % i), "wb").write(_run(reducer, data, counters))
    print("[local-mr] %-8s %s" % (name, counters), file=sys.stderr)
    return counters


def parts(d):
    return sorted(glob.glob(os.path.join(d, "part-*")))


def run_pipeline(raw_inputs, work, reduces=4):
    c = {}
    c.update(run_job("P", raw_inputs, os.path.join(work, "prep"), "prep_mapper.py"))
    prep = parts(os.path.join(work, "prep"))
    c.update(run_job("D1", prep, os.path.join(work, "buckets"), "lsh_mapper.py", "bucket_reducer.py", reduces))
    c.update(run_job("D2", parts(os.path.join(work, "buckets")), os.path.join(work, "pairs"),
                     "cat", "dedup_reducer.py", reduces, key_fields=2))
    c.update(run_job("D3", prep + parts(os.path.join(work, "pairs")), os.path.join(work, "joined"),
                     "join_mapper.py", "join_reducer.py", reduces))
    c.update(run_job("D4", parts(os.path.join(work, "joined")), os.path.join(work, "similar"),
                     "cat", "verify_reducer.py", reduces, key_fields=2))
    return c


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", nargs="+", required=True)
    ap.add_argument("--work", required=True)
    ap.add_argument("--reduces", type=int, default=4)
    a = ap.parse_args()
    print(run_pipeline(a.input, a.work, a.reduces))
